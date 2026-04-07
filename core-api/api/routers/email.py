"""
Email router - HTTP endpoints for email operations
"""
import asyncio
from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, EmailStr
from api.services.email import (
    fetch_emails,
    get_email_details,
    get_email_attachment,
    get_thread_emails,
    send_email,
    reply_to_email,
    forward_email,
    create_draft,
    update_draft,
    send_draft,
    delete_draft,
    delete_email,
    restore_email,
    archive_email,
    apply_labels,
    remove_labels,
    get_labels,
    mark_as_read,
    mark_as_unread,
    search_emails_with_providers,
    fetch_remote_email,
)
from api.dependencies import get_current_user_jwt, get_current_user_id
from api.exceptions import handle_api_exception
from lib.supabase_client import get_authenticated_supabase_client, get_authenticated_async_client
from lib.token_encryption import decrypt_ext_connection_tokens
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/email", tags=["email"])


# ============================================================================
# Response Models
# ============================================================================

class EmailItemResponse(BaseModel):
    """Response model for a single email."""
    id: Optional[str] = None
    external_id: Optional[str] = None
    thread_id: Optional[str] = None
    subject: Optional[str] = None
    snippet: Optional[str] = None
    body: Optional[str] = None
    sender: Optional[str] = None
    to_recipients: Optional[str] = None
    cc_recipients: Optional[str] = None
    label_ids: Optional[List[str]] = None
    is_read: Optional[bool] = None
    is_starred: Optional[bool] = None
    received_at: Optional[str] = None
    account_email: Optional[str] = None
    account_provider: Optional[str] = None
    connection_id: Optional[str] = None
    ai_category: Optional[str] = None
    ai_summary: Optional[str] = None
    ai_priority: Optional[str] = None

    class Config:
        extra = "allow"


class EmailListResponse(BaseModel):
    """Response model for email list."""
    emails: List[EmailItemResponse]
    count: int
    unified: Optional[bool] = None
    accounts_status: Optional[List[dict]] = None

    class Config:
        extra = "allow"


class EmailAccountCount(BaseModel):
    """Per-account email count."""
    id: str
    email: Optional[str] = None
    provider: Optional[str] = None
    inbox_unread: int = 0
    drafts_count: int = 0


class EmailCountsResponse(BaseModel):
    """Response model for email counts."""
    inbox_unread: int
    drafts_count: int
    unified: bool
    per_account: List[EmailAccountCount]


class EmailSendResponse(BaseModel):
    """Response model for email send/reply/forward operations."""
    success: Optional[bool] = None
    message_id: Optional[str] = None
    thread_id: Optional[str] = None

    class Config:
        extra = "allow"


class EmailThreadResponse(BaseModel):
    """Response model for email thread."""
    emails: List[EmailItemResponse]
    count: int

    class Config:
        extra = "allow"


class EmailLabelItem(BaseModel):
    """A single email label."""
    id: str
    name: str
    type: Optional[str] = None

    class Config:
        extra = "allow"


class EmailLabelsResponse(BaseModel):
    """Response model for email labels list."""
    labels: List[EmailLabelItem]
    count: int


class EmailAttachmentResponse(BaseModel):
    """Response model for email attachment download."""
    attachment: dict

    class Config:
        extra = "allow"


class EmailActionResponse(BaseModel):
    """Response model for email actions (archive, read, unread, labels)."""
    success: Optional[bool] = None
    message: Optional[str] = None

    class Config:
        extra = "allow"


class EmailSyncResponse(BaseModel):
    """Response model for email sync."""
    status: Optional[str] = None
    new_emails: Optional[int] = None
    updated_emails: Optional[int] = None
    ai_analyzed_count: Optional[int] = None
    jobs_enqueued: Optional[int] = None
    streams_marked: Optional[int] = None

    class Config:
        extra = "allow"


class EmailAnalyzeResponse(BaseModel):
    """Response model for email analysis."""
    status: str
    analyzed_count: int
    message: str


# ============================================================================
# Request Models
# ============================================================================

class SendEmailRequest(BaseModel):
    to: str
    subject: str
    body: str
    cc: Optional[List[str]] = None
    bcc: Optional[List[str]] = None
    html_body: Optional[str] = None
    thread_id: Optional[str] = None
    in_reply_to: Optional[str] = None  # Message-ID of email being replied to
    references: Optional[str] = None   # Chain of Message-IDs for threading
    attachments: Optional[List["AttachmentUpload"]] = None
    from_account_id: Optional[str] = None  # Which account to send from (multi-account)


class CreateDraftRequest(BaseModel):
    to: Optional[str] = None
    subject: str = ""
    body: str = ""
    cc: Optional[List[str]] = None
    bcc: Optional[List[str]] = None
    html_body: Optional[str] = None
    attachments: Optional[List["AttachmentUpload"]] = None
    account_id: Optional[str] = None


class UpdateDraftRequest(BaseModel):
    to: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    cc: Optional[List[str]] = None
    bcc: Optional[List[str]] = None
    html_body: Optional[str] = None
    attachments: Optional[List["AttachmentUpload"]] = None


class ApplyLabelsRequest(BaseModel):
    label_names: List[str]


class ReplyEmailRequest(BaseModel):
    body: str
    html_body: Optional[str] = None
    reply_all: bool = False


class ForwardEmailRequest(BaseModel):
    to: EmailStr
    additional_message: Optional[str] = None
    cc: Optional[List[EmailStr]] = None
    include_attachments: bool = True


class AttachmentUpload(BaseModel):
    filename: str
    content: str  # Base64-encoded
    mime_type: str = "application/octet-stream"


class EmailSearchRequest(BaseModel):
    query: str
    account_ids: Optional[List[str]] = None
    provider_search: bool = True
    max_results: int = 25


class EmailSearchResponse(BaseModel):
    emails: List[Dict[str, Any]]
    count: int
    local_count: int
    remote_count: int
    query: str
    provider_errors: Optional[Dict[str, str]] = None
    has_provider_errors: bool


class FetchRemoteRequest(BaseModel):
    external_id: str
    connection_id: str


class FetchRemoteResponse(BaseModel):
    success: bool
    email: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# Rebuild models for forward references
SendEmailRequest.model_rebuild()
CreateDraftRequest.model_rebuild()
UpdateDraftRequest.model_rebuild()


def _convert_attachments(attachments: Optional[List[AttachmentUpload]]) -> Optional[List[dict]]:
    """Convert attachment models to dict format for service layer."""
    if not attachments:
        return None
    return [
        {
            "filename": att.filename,
            "content": att.content,
            "mime_type": att.mime_type
        }
        for att in attachments
    ]


# Email fetch endpoints
@router.get("/messages", response_model=EmailListResponse)
async def fetch_emails_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    max_results: int = 50,  # Reduced default for better performance
    offset: int = 0,  # Pagination offset
    query: Optional[str] = None,
    label_ids: List[str] = Query(default=[]),
    account_id: Optional[str] = Query(None, description="Filter by single email account (backward compatible)"),
    account_ids: List[str] = Query(default=[], description="Filter by multiple email accounts (unified view filter chips)")
):
    """
    Fetch emails from database with optional filtering and pagination.

    Query params:
    - max_results: Maximum number of emails to return (default 50)
    - offset: Number of emails to skip for pagination (default 0)
    - label_ids: Filter by Gmail labels (e.g., INBOX, SENT, DRAFT)
    - account_id: Filter by single email account (backward compatible)
    - account_ids: Filter by multiple email accounts (for filter chips)

    Unified View Behavior:
    - No account_id/account_ids: Returns emails from ALL accounts (unified view)
    - account_id provided: Returns emails from single account (backward compatible)
    - account_ids provided: Returns emails from specified accounts (filter chips)

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        # Convert empty list to None for the service function
        effective_labels = label_ids if label_ids else None

        # Handle account filtering: prioritize account_ids over account_id
        effective_account_ids = None
        if account_ids:
            effective_account_ids = account_ids
        elif account_id:
            effective_account_ids = [account_id]
        # If both are None/empty, unified view (all accounts)

        logger.info(f"📧 Fetching emails for user {user_id} with labels={effective_labels}, offset={offset}, limit={max_results}, account_ids={effective_account_ids}")
        result = await fetch_emails(
            user_id,
            user_jwt,
            max_results,
            query,
            label_ids=effective_labels,
            offset=offset,
            account_ids=effective_account_ids
        )
        logger.info(f"✅ Fetched {result.get('count', 0)} emails (unified={result.get('unified', False)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch emails", logger)


@router.get("/counts", response_model=EmailCountsResponse)
async def get_email_counts_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    account_id: Optional[str] = Query(None, description="Filter by single email account (backward compatible)"),
    account_ids: List[str] = Query(default=[], description="Filter by multiple email accounts")
):
    """
    Get email counts efficiently without fetching full email data.
    Returns unread count for inbox and total count for drafts.

    Query params:
    - account_id: Filter by single email account (backward compatible)
    - account_ids: Filter by multiple email accounts

    Unified View Behavior:
    - No account_id/account_ids: Returns counts from ALL accounts (unified) + per-account breakdown
    - account_id provided: Returns counts from single account (backward compatible)
    - account_ids provided: Returns counts from specified accounts

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        auth_supabase = await get_authenticated_async_client(user_jwt)

        # Handle account filtering
        effective_account_ids = None
        if account_ids:
            effective_account_ids = account_ids
        elif account_id:
            effective_account_ids = [account_id]
        # If both are None/empty, unified view (all accounts)

        is_unified = effective_account_ids is None
        logger.info(f"📧 Fetching email counts for user {user_id}, account_ids={effective_account_ids}, unified={is_unified}")

        # Use efficient RPC to get all counts in a single query (fixes N+1 problem)
        counts_result = await auth_supabase.rpc(
            'get_email_counts_by_account',
            {
                'p_user_id': user_id,
                'p_account_ids': effective_account_ids  # NULL = all accounts
            }
        ).execute()

        # Build response from RPC results
        per_account = []
        total_inbox_unread = 0
        total_drafts = 0

        for row in counts_result.data or []:
            inbox_unread = row.get('inbox_unread_count', 0)
            drafts_count = row.get('drafts_count', 0)

            per_account.append({
                "id": row['account_id'],
                "email": row['provider_email'],
                "provider": row['provider'],
                "inbox_unread": inbox_unread,
                "drafts_count": drafts_count
            })

            total_inbox_unread += inbox_unread
            total_drafts += drafts_count

        logger.info(f"✅ Counts: total_inbox_unread={total_inbox_unread}, total_drafts={total_drafts}, accounts={len(per_account)}")

        return {
            "inbox_unread": total_inbox_unread,
            "drafts_count": total_drafts,
            "unified": is_unified,
            "per_account": per_account
        }
    except Exception as e:
        handle_api_exception(e, "Failed to fetch email counts", logger)


@router.get("/messages/{email_id}", response_model=EmailItemResponse)
async def get_email_details_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get full details of a specific email including body content.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Fetching email details for {email_id}")
        result = await asyncio.to_thread(get_email_details, user_id, user_jwt, email_id)
        logger.info("✅ Email details retrieved")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch email details", logger)


@router.get("/messages/{email_id}/attachments/{attachment_id}", response_model=EmailAttachmentResponse)
async def get_attachment_endpoint(
    email_id: str,
    attachment_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Download a specific attachment from an email.
    Returns base64url-encoded attachment data.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📎 Fetching attachment {attachment_id} from email {email_id}")
        # Use asyncio.to_thread to avoid blocking the event loop
        result = await asyncio.to_thread(
            get_email_attachment, user_id, user_jwt, email_id, attachment_id
        )
        if not result or 'attachment' not in result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")
        logger.info("✅ Attachment retrieved successfully")
        return result
    except HTTPException:
        raise
    except Exception as e:
        handle_api_exception(e, "Failed to fetch attachment", logger)


@router.get("/threads/{thread_id}", response_model=EmailThreadResponse)
async def get_thread_endpoint(
    thread_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get all emails in a thread (conversation view).
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Fetching thread {thread_id} for user {user_id}")
        result = await get_thread_emails(user_id, user_jwt, thread_id)
        logger.info(f"✅ Thread retrieved with {result.get('count', 0)} messages")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch thread", logger)


# Send email endpoints
@router.post("/send", response_model=EmailSendResponse)
async def send_email_endpoint(
    email_data: SendEmailRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Send an email via Gmail.
    
    For replies, include thread_id. The backend will automatically fetch
    In-Reply-To and References headers if not provided. You can also 
    explicitly pass in_reply_to (Message-ID) for more reliable threading.
    
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Sending email for user {user_id}")
        if email_data.thread_id:
            logger.info(f"   Thread ID: {email_data.thread_id}")
            logger.info(f"   In-Reply-To: {email_data.in_reply_to or '(will auto-fetch)'}")

        # Convert attachments to dict format for service
        attachments = _convert_attachments(email_data.attachments)
        if attachments:
            logger.info(f"   Attachments: {len(attachments)} files")

        result = send_email(
            user_id=user_id,
            user_jwt=user_jwt,
            to=email_data.to,
            subject=email_data.subject,
            body=email_data.body,
            cc=email_data.cc,
            bcc=email_data.bcc,
            html_body=email_data.html_body,
            thread_id=email_data.thread_id,
            in_reply_to=email_data.in_reply_to,
            references=email_data.references,
            attachments=attachments,
            from_account_id=email_data.from_account_id
        )
        logger.info("✅ Email sent successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to send email", logger)


@router.post("/messages/{email_id}/reply", response_model=EmailSendResponse)
async def reply_to_email_endpoint(
    email_id: str,
    reply_data: ReplyEmailRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Reply to an existing email.

    This endpoint handles all the threading automatically:
    - Fetches the original email's Message-ID for In-Reply-To header
    - Builds the References chain for proper threading
    - Adds "Re: " prefix to subject if not already present
    - For reply_all=true, includes all original recipients in CC

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Replying to email {email_id} for user {user_id} (reply_all={reply_data.reply_all})")
        result = reply_to_email(
            user_id=user_id,
            user_jwt=user_jwt,
            original_email_id=email_id,
            body=reply_data.body,
            html_body=reply_data.html_body,
            reply_all=reply_data.reply_all
        )
        logger.info("✅ Reply sent successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to reply to email", logger)


@router.post("/messages/{email_id}/forward", response_model=EmailSendResponse)
async def forward_email_endpoint(
    email_id: str,
    forward_data: ForwardEmailRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Forward an existing email to new recipients.

    This endpoint:
    - Fetches the original email content
    - Adds "Fwd: " prefix to subject if not already present
    - Includes original email metadata (From, Date, Subject, To) in forwarded body
    - Optionally prepends your additional message
    - Optionally includes original attachments (default: true)

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Forwarding email {email_id} to {forward_data.to} for user {user_id}")
        if forward_data.include_attachments:
            logger.info("   Including original attachments")
        result = forward_email(
            user_id=user_id,
            user_jwt=user_jwt,
            original_email_id=email_id,
            to=forward_data.to,
            additional_message=forward_data.additional_message,
            cc=forward_data.cc,
            include_attachments=forward_data.include_attachments
        )
        logger.info("✅ Email forwarded successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to forward email", logger)


# Draft endpoints
@router.post("/drafts", response_model=EmailSendResponse)
async def create_draft_endpoint(
    draft_data: CreateDraftRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Create a draft email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Creating draft for user {user_id}")

        # Convert attachments to dict format for service
        attachments = _convert_attachments(draft_data.attachments)
        if attachments:
            logger.info(f"   Attachments: {len(attachments)} files")

        result = create_draft(
            user_id=user_id,
            user_jwt=user_jwt,
            to=draft_data.to,
            subject=draft_data.subject,
            body=draft_data.body,
            cc=draft_data.cc,
            bcc=draft_data.bcc,
            html_body=draft_data.html_body,
            attachments=attachments,
            account_id=draft_data.account_id
        )
        logger.info("✅ Draft created successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to create draft", logger)


@router.put("/drafts/{draft_id}", response_model=EmailSendResponse)
async def update_draft_endpoint(
    draft_id: str,
    draft_data: UpdateDraftRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Update an existing draft email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Updating draft {draft_id} for user {user_id}")
        result = update_draft(
            user_id=user_id,
            user_jwt=user_jwt,
            draft_id=draft_id,
            to=draft_data.to,
            subject=draft_data.subject,
            body=draft_data.body,
            cc=draft_data.cc,
            bcc=draft_data.bcc,
            html_body=draft_data.html_body
        )
        logger.info("✅ Draft updated successfully")
        return result
    except Exception as e:
        # Draft IDs can become stale (already sent/deleted); surface as 404 instead of generic 500.
        handle_api_exception(e, "Failed to update draft", logger, check_not_found=True)


@router.patch("/drafts/{draft_id}", response_model=EmailSendResponse)
async def update_draft_patch_endpoint(
    draft_id: str,
    draft_data: UpdateDraftRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Backward-compatible alias for clients still using PATCH.
    """
    return await update_draft_endpoint(
        draft_id=draft_id,
        draft_data=draft_data,
        user_jwt=user_jwt,
        user_id=user_id
    )


@router.delete("/drafts/{draft_id}", response_model=EmailActionResponse)
async def delete_draft_endpoint(
    draft_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Delete a draft email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Deleting draft {draft_id} for user {user_id}")
        result = delete_draft(user_id, user_jwt, draft_id)
        logger.info("✅ Draft deleted successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to delete draft", logger, check_not_found=True)


@router.post("/drafts/{draft_id}/send", response_model=EmailSendResponse)
async def send_draft_endpoint(
    draft_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Send an existing draft email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Sending draft {draft_id} for user {user_id}")
        result = send_draft(
            user_id=user_id,
            user_jwt=user_jwt,
            draft_id=draft_id
        )
        logger.info("✅ Draft sent successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to send draft", logger, check_not_found=True)


# Email action endpoints
@router.delete("/messages/{email_id}", response_model=EmailActionResponse)
async def delete_email_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Delete an email (move to trash).
    Note: Emails are moved to trash, not permanently deleted, to comply with Gmail API scopes.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Moving email {email_id} to trash for user {user_id}")
        result = delete_email(user_id, user_jwt, email_id)
        logger.info("✅ Email moved to trash successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to delete email", logger)


@router.post("/messages/{email_id}/restore", response_model=EmailActionResponse)
async def restore_email_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Restore an email from trash.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Restoring email {email_id} from trash for user {user_id}")
        result = restore_email(user_id, user_jwt, email_id)
        logger.info("✅ Email restored from trash successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to restore email", logger)


@router.post("/messages/{email_id}/archive", response_model=EmailActionResponse)
async def archive_email_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Archive an email (remove from inbox).
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Archiving email {email_id} for user {user_id}")
        result = archive_email(user_id, user_jwt, email_id)
        logger.info("✅ Email archived successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to archive email", logger)


@router.post("/messages/{email_id}/mark-read", response_model=EmailActionResponse)
async def mark_read_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Mark an email as read.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Marking email {email_id} as read for user {user_id}")
        result = mark_as_read(user_id, user_jwt, email_id)
        logger.info("✅ Email marked as read")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to mark email as read", logger)


@router.post("/messages/{email_id}/mark-unread", response_model=EmailActionResponse)
async def mark_unread_endpoint(
    email_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Mark an email as unread.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Marking email {email_id} as unread for user {user_id}")
        result = mark_as_unread(user_id, user_jwt, email_id)
        logger.info("✅ Email marked as unread")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to mark email as unread", logger)


# Label endpoints
@router.get("/labels", response_model=EmailLabelsResponse)
async def get_labels_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Get all available Gmail labels for the user.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Fetching labels for user {user_id}")
        result = get_labels(user_id, user_jwt)
        logger.info(f"✅ Fetched {result.get('count', 0)} labels")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch labels", logger)


@router.post("/messages/{email_id}/labels", response_model=EmailActionResponse)
async def apply_labels_endpoint(
    email_id: str,
    labels_data: ApplyLabelsRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Apply labels to an email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Applying labels to email {email_id} for user {user_id}")
        result = apply_labels(user_id, user_jwt, email_id, labels_data.label_names)
        logger.info("✅ Labels applied successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to apply labels", logger)


@router.delete("/messages/{email_id}/labels", response_model=EmailActionResponse)
async def remove_labels_endpoint(
    email_id: str,
    labels_data: ApplyLabelsRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Remove labels from an email.
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📧 Removing labels from email {email_id} for user {user_id}")
        result = remove_labels(user_id, user_jwt, email_id, labels_data.label_names)
        logger.info("✅ Labels removed successfully")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to remove labels", logger)


# Provider search endpoints
@router.post("/search", response_model=EmailSearchResponse)
async def search_emails_endpoint(
    search_data: EmailSearchRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Search emails across local database and provider APIs (Gmail, Microsoft Graph).

    Provider-side search enables searching the user's entire mailbox, not just
    locally synced emails (~20 days). Results are merged and deduplicated.

    Body params:
    - query: Search query (supports operators like from:, has:attachment, after:, etc.)
    - account_ids: Optional list of ext_connection_ids to search
    - provider_search: Whether to also search provider APIs (default True)
    - max_results: Maximum results to return (default 25)

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"[Search] user={user_id[:8]}... query='{search_data.query}' provider={search_data.provider_search}")
        result = await search_emails_with_providers(
            user_id=user_id,
            user_jwt=user_jwt,
            query=search_data.query,
            account_ids=search_data.account_ids,
            provider_search=search_data.provider_search,
            max_results=search_data.max_results,
        )
        logger.info(f"[Search] Done: {result.get('count', 0)} results ({result.get('local_count', 0)} local, {result.get('remote_count', 0)} remote)")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to search emails", logger)


@router.post("/fetch-remote", response_model=FetchRemoteResponse)
async def fetch_remote_email_endpoint(
    fetch_data: FetchRemoteRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Fetch a single email from provider API and sync it to local database.

    Used when a user clicks on a provider-only search result that hasn't
    been synced locally yet. Fetches full content and upserts to DB.

    Body params:
    - external_id: Provider's message ID
    - connection_id: ext_connection_id for the account

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"[Fetch Remote] user={user_id[:8]}... external_id={fetch_data.external_id[:16]}...")
        email = await fetch_remote_email(
            user_id=user_id,
            user_jwt=user_jwt,
            external_id=fetch_data.external_id,
            connection_id=fetch_data.connection_id,
        )
        logger.info("[Fetch Remote] Success")
        return {"success": True, "email": email}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error(f"[Fetch Remote] Error: {e}")
        handle_api_exception(e, "Failed to fetch remote email", logger)


# Sync endpoint
@router.post("/sync", response_model=EmailSyncResponse, status_code=200)
async def sync_emails_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Sync emails from connected providers (Google and Microsoft).

    Marks per-connection email streams dirty and returns 202 immediately.

    Sync execution happens on the background worker plane.

    Requires: Authorization header with user's Supabase JWT
    """
    logger.info(f"📧 Email sync requested for user {user_id[:8]}...")

    from lib.supabase_client import get_service_role_client
    from fastapi.responses import JSONResponse
    from api.services.syncs.sync_dispatcher import MANUAL_SYNC_PRIORITY, mark_stream_dirty
    from api.services.syncs.sync_state_store import SYNC_KIND_EMAIL

    try:
        auth_supabase = get_authenticated_supabase_client(user_jwt)
        service_supabase = get_service_role_client()
        connections_result = auth_supabase.table('ext_connections')\
            .select('id, provider, provider_email, access_token, refresh_token, token_expires_at, metadata, is_primary')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .in_('provider', ['google', 'microsoft'])\
            .execute()

        connections = [decrypt_ext_connection_tokens(c) for c in (connections_result.data or [])]
        google_connections = [c for c in connections if c.get('provider') == 'google']
        microsoft_connections = [c for c in connections if c.get('provider') == 'microsoft']

        if not google_connections and not microsoft_connections:
            raise ValueError("No active email connection found. Please sign in with Google or Microsoft first.")

        streams_marked = 0

        for conn in google_connections:
            if mark_stream_dirty(
                service_supabase,
                conn['id'],
                "google",
                SYNC_KIND_EMAIL,
                priority=MANUAL_SYNC_PRIORITY,
                metadata={"source": "manual-email-sync"},
            ):
                streams_marked += 1

        for conn in microsoft_connections:
            if conn.get('id'):
                if mark_stream_dirty(
                    service_supabase,
                    conn['id'],
                    "microsoft",
                    SYNC_KIND_EMAIL,
                    priority=MANUAL_SYNC_PRIORITY,
                    metadata={"source": "manual-email-sync"},
                ):
                    streams_marked += 1

        if streams_marked <= 0:
            # streams_marked==0 with connections means already dirty/leased — still 202
            logger.info(f"ℹ️ All email streams already scheduled for user {user_id[:8]}...")

        logger.info(f"✅ Marked {streams_marked} email streams dirty for user {user_id[:8]}...")
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "jobs_enqueued": streams_marked,
                "streams_marked": streams_marked,
                "new_emails": 0,
                "updated_emails": 0,
            }
        )
    except Exception as e:
        handle_api_exception(e, "Failed to sync emails", logger)


# AI Analysis endpoint
@router.post("/analyze", response_model=EmailAnalyzeResponse)
async def analyze_emails_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id)
):
    """
    Manually trigger AI analysis for unanalyzed emails.
    Analyzes up to 50 emails at a time.
    Requires: Authorization header with user's Supabase JWT
    """
    from api.services.email.analyze_email_ai import analyze_unanalyzed_emails
    
    try:
        logger.info(f"🤖 Starting AI analysis for user {user_id}")
        analyzed_count = await asyncio.to_thread(analyze_unanalyzed_emails, user_id=user_id, limit=50)
        logger.info(f"✅ AI analysis completed for user {user_id}: {analyzed_count} emails")
        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "message": f"Successfully analyzed {analyzed_count} emails"
        }
    except Exception as e:
        handle_api_exception(e, "Failed to analyze emails", logger)
