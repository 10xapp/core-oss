"""Service for retrieving documents."""
from typing import Optional, List, Literal, Dict, Any, cast
from fastapi import HTTPException, status
from lib.supabase_client import get_authenticated_async_client, get_async_service_role_client
from lib.image_proxy import generate_file_url, is_image_type
from api.config import settings
from api.services.users import get_public_profiles_by_ids
import logging

logger = logging.getLogger(__name__)


def _enrich_documents_with_image_urls(documents: List[dict]) -> None:
    """Add thumb_url/preview_url/file_url for image-type documents.

    Only enriches when the image proxy is configured. Mutates in-place.
    """
    if not settings.image_proxy_url or not settings.image_proxy_secret:
        return

    for doc in documents:
        file_data = doc.get("file")
        if not file_data or not isinstance(file_data, dict):
            continue
        r2_key = file_data.get("r2_key")
        mime = file_data.get("file_type", "")
        if not r2_key:
            continue
        if is_image_type(mime):
            doc["thumb_url"] = generate_file_url(r2_key, mime, "thumb")
            doc["preview_url"] = generate_file_url(r2_key, mime, "preview")
            doc["file_url"] = generate_file_url(r2_key, mime, "full")

# Valid sort options
SortBy = Literal["name", "type", "date", "size", "position"]
SortDirection = Literal["asc", "desc"]


async def get_documents(
    user_id: str,
    user_jwt: str,
    parent_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    workspace_app_id: Optional[str] = None,
    workspace_ids: Optional[List[str]] = None,
    include_archived: bool = False,
    favorites_only: bool = False,
    folders_only: bool = False,
    documents_only: bool = False,
    tags: Optional[List[str]] = None,
    sort_by: SortBy = "date",
    sort_direction: SortDirection = "desc",
    fetch_all: bool = False,
) -> List[dict]:
    """
    Get documents for a user.

    Args:
        user_id: User ID
        user_jwt: User's Supabase JWT for authenticated requests
        parent_id: Filter by parent document ID (None for root documents)
        workspace_id: Optional workspace ID to filter by
        workspace_app_id: Optional workspace app ID to filter by
        include_archived: Whether to include archived documents
        favorites_only: Only return favorite documents
        folders_only: Only return folders
        documents_only: Only return documents (not folders)
        tags: Filter by tags (returns docs with ANY of these tags)
        sort_by: Sort field - "name", "type", "date", or "size"
        sort_direction: Sort direction - "asc" or "desc"

    Returns:
        List of documents
    """
    auth_supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Build query - if workspace filter provided, rely on RLS for access control
        # Otherwise filter by user_id for personal documents
        query = auth_supabase.table("documents").select("*, file:files(*)")

        # Apply workspace filters if provided to narrow scope.
        # RLS handles access control in all cases — only accessible documents are returned.
        if workspace_app_id:
            query = query.eq("workspace_app_id", workspace_app_id)
        elif workspace_ids:
            query = query.in_("workspace_id", workspace_ids)
        elif workspace_id:
            query = query.eq("workspace_id", workspace_id)
        # If no workspace filter, RLS still limits to accessible documents
        # (owned, workspace member, or shared via permissions)

        # Filter by parent_id - None means root level (parent_id IS NULL)
        # When fetch_all=True, skip parent_id filter to return all documents
        if not fetch_all:
            if parent_id is not None:
                query = query.eq("parent_id", parent_id)
            else:
                query = query.is_("parent_id", "null")

        # Filter archived
        if not include_archived:
            query = query.eq("is_archived", False)

        # Filter favorites
        if favorites_only:
            query = query.eq("is_favorite", True)

        # Filter by type
        if folders_only:
            query = query.eq("is_folder", True)
        elif documents_only:
            query = query.eq("is_folder", False)

        # Filter by tags - using overlap operator (&&) to find docs with ANY matching tag
        if tags:
            query = query.overlaps("tags", tags)

        # Determine sort direction
        is_desc = sort_direction == "desc"

        # Apply sorting based on sort_by parameter
        # Map sort_by to database column(s)
        sort_column_map = {
            "name": "title",
            "type": "type",
            "date": "updated_at",
            "size": None,  # Handled specially - needs Python sort
            "position": "position",
        }

        sort_column = sort_column_map.get(sort_by, "updated_at")

        if sort_by == "size":
            # For size sorting, we need to fetch and sort in Python
            # because PostgREST doesn't support ordering by joined table columns directly
            query = query.order("updated_at", desc=is_desc)
            result = await query.execute()
            documents = result.data

            # Sort all items by file size (folders/notes have size 0)
            def get_file_size(doc: dict) -> int:
                file_data = doc.get("file")
                if file_data and isinstance(file_data, dict):
                    return file_data.get("file_size", 0) or 0
                return 0

            documents.sort(key=get_file_size, reverse=is_desc)

            logger.info(f"Retrieved {len(documents)} documents for user {user_id} (sorted by size)")
            _enrich_documents_with_image_urls(documents)
            return documents
        else:
            # Sort by the specified column
            query = query.order(sort_column, desc=is_desc)
            result = await query.execute()

            logger.info(f"Retrieved {len(result.data)} documents for user {user_id}")
            _enrich_documents_with_image_urls(result.data)
            return result.data

    except Exception as e:
        logger.error(f"Error retrieving documents: {str(e)}")
        raise


async def get_document_by_id(user_id: str, user_jwt: str, document_id: str) -> Optional[dict]:
    """
    Get a specific document by ID.
    
    This will return the document if:
    1. User owns the document, OR
    2. Document is shared with the user
    
    Only updates last_opened_at for owned documents.
    
    Args:
        user_id: User ID
        user_jwt: User's Supabase JWT for authenticated requests
        document_id: Document ID
    
    Returns:
        Document record or None if not found/no access
    """
    auth_supabase = await get_authenticated_async_client(user_jwt)

    try:
        # First, try to get the document (RLS will check access)
        # Don't filter by user_id - let RLS handle access control
        # RLS allows SELECT if user owns it OR has workspace app access (can_access_workspace_app)
        result = await (
            auth_supabase.table("documents")
            .select("*, file:files(*)")
            .eq("id", document_id)
            .execute()
        )

        if not result.data:
            # RLS returned nothing — check if document actually exists (service role bypasses RLS)
            from lib.supabase_client import get_async_service_role_client
            sr = await get_async_service_role_client()
            exists = await sr.table("documents").select("id").eq("id", document_id).maybe_single().execute()
            if exists.data:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You don't have access to this document")
            return None

        doc = cast(Dict[str, Any], result.data[0])
        if doc.get("user_id"):
            owner_map = await get_public_profiles_by_ids([doc["user_id"]])
            doc["owner"] = owner_map.get(doc["user_id"], {"id": doc["user_id"]})

        # Update last_opened_at
        from datetime import datetime, timezone
        await auth_supabase.table("documents").update({
            "last_opened_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", document_id).eq("user_id", user_id).execute()

        _enrich_documents_with_image_urls([doc])
        return doc
        
    except Exception as e:
        logger.error(f"Error retrieving document {document_id}: {str(e)}")
        raise


async def assert_document_access(
    user_id: str,
    user_jwt: str,
    document_id: str,
) -> None:
    """Raise if the user cannot read the target document row.

    This is the same access contract as ``get_document_by_id`` but without the
    ``last_opened_at`` side effect, so it is safe for authorization-only checks
    such as notification subscription gating.
    """
    auth_supabase = await get_authenticated_async_client(user_jwt)

    try:
        result = await (
            auth_supabase.table("documents")
            .select("id")
            .eq("id", document_id)
            .limit(1)
            .execute()
        )

        if result.data:
            return

        sr = await get_async_service_role_client()
        exists = await sr.table("documents").select("id").eq("id", document_id).maybe_single().execute()
        if exists.data:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have access to this document",
            )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking access for document {document_id}: {str(e)}")
        raise
