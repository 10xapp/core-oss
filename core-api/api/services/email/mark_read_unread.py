"""
Email service - Mark as read/unread/starred operations
Supports both Gmail and Outlook with two-way sync.

Gmail: Uses label-based system (UNREAD/STARRED are labels)
Outlook: Uses property-based system (isRead/flag.flagStatus)

This module handles the translation between these models.
"""
from typing import Dict, Any
from lib.supabase_client import get_authenticated_supabase_client, get_service_role_client
from lib.token_encryption import decrypt_ext_connection_tokens
import logging
import requests
from googleapiclient.errors import HttpError
from .google_api_helpers import get_gmail_service
from api.services.microsoft.microsoft_oauth_provider import get_valid_microsoft_credentials

logger = logging.getLogger(__name__)

# Microsoft Graph API base URL
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"


def _get_email_provider(user_id: str, email_id: str, user_jwt: str) -> tuple[str, str, dict]:
    """
    Get the provider and connection info for an email.

    Args:
        user_id: User's ID
        email_id: External email ID (Gmail message ID or Outlook message ID)
        user_jwt: User's Supabase JWT

    Returns:
        Tuple of (provider, connection_id, connection_data)
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get email with its connection info
    result = auth_supabase.table('emails')\
        .select('ext_connection_id, ext_connections(id, provider, access_token, refresh_token, token_expires_at, metadata)')\
        .eq('user_id', user_id)\
        .eq('external_id', email_id)\
        .single()\
        .execute()

    if not result.data:
        raise ValueError(f"Email not found: {email_id}")

    connection = decrypt_ext_connection_tokens(result.data.get('ext_connections', {}))
    provider = connection.get('provider', 'google')
    connection_id = connection.get('id')

    # Build connection_data for token refresh
    connection_data = {
        'id': connection_id,
        'access_token': connection.get('access_token'),
        'refresh_token': connection.get('refresh_token'),
        'token_expires_at': connection.get('token_expires_at'),
        'metadata': connection.get('metadata', {}),
    }

    return provider, connection_id, connection_data


def _update_outlook_read_status(
    email_id: str,
    is_read: bool,
    connection_data: dict
) -> bool:
    """
    Update read status in Outlook via Microsoft Graph API.

    Args:
        email_id: Outlook message ID
        is_read: True to mark as read, False for unread
        connection_data: Connection data with tokens

    Returns:
        True if successful
    """
    supabase = get_service_role_client()
    access_token = get_valid_microsoft_credentials(connection_data, supabase)

    url = f"{GRAPH_API_URL}/me/messages/{email_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.patch(
        url,
        headers=headers,
        json={"isRead": is_read},
        timeout=30
    )

    if response.status_code not in [200, 204]:
        error_msg = response.text
        try:
            error_msg = response.json().get('error', {}).get('message', response.text)
        except Exception:
            pass
        raise ValueError(f"Failed to update Outlook email: {error_msg}")

    return True


def _update_outlook_flag_status(
    email_id: str,
    is_starred: bool,
    connection_data: dict
) -> bool:
    """
    Update flag (starred) status in Outlook via Microsoft Graph API.

    Args:
        email_id: Outlook message ID
        is_starred: True to flag, False to unflag
        connection_data: Connection data with tokens

    Returns:
        True if successful
    """
    supabase = get_service_role_client()
    access_token = get_valid_microsoft_credentials(connection_data, supabase)

    url = f"{GRAPH_API_URL}/me/messages/{email_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Outlook uses flag.flagStatus: "flagged" or "notFlagged"
    flag_status = "flagged" if is_starred else "notFlagged"

    response = requests.patch(
        url,
        headers=headers,
        json={"flag": {"flagStatus": flag_status}},
        timeout=30
    )

    if response.status_code not in [200, 204]:
        error_msg = response.text
        try:
            error_msg = response.json().get('error', {}).get('message', response.text)
        except Exception:
            pass
        raise ValueError(f"Failed to update Outlook email flag: {error_msg}")

    return True


def _update_labels_array(labels: list, add: list = None, remove: list = None) -> list:
    """
    Update labels array by adding/removing labels.

    Args:
        labels: Current labels array
        add: Labels to add
        remove: Labels to remove

    Returns:
        Updated labels array
    """
    result = set(labels or [])

    if remove:
        for label in remove:
            result.discard(label)

    if add:
        for label in add:
            result.add(label)

    return sorted(list(result))


def mark_as_read(
    user_id: str,
    user_jwt: str,
    email_id: str
) -> Dict[str, Any]:
    """
    Mark an email as read.
    Two-way sync with database and provider (Gmail/Outlook).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID to mark as read

    Returns:
        Dict with confirmation
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get provider info
    provider, connection_id, connection_data = _get_email_provider(user_id, email_id, user_jwt)

    try:
        if provider == 'microsoft':
            # Update Outlook via Graph API
            _update_outlook_read_status(email_id, is_read=True, connection_data=connection_data)

            # Get current labels and remove UNREAD
            email_result = auth_supabase.table('emails')\
                .select('labels')\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .single()\
                .execute()

            current_labels = email_result.data.get('labels', []) if email_result.data else []
            new_labels = _update_labels_array(current_labels, remove=['UNREAD'])

            # Update database
            auth_supabase.table('emails')\
                .update({
                    'is_read': True,
                    'labels': new_labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            logger.info(f"✅ Marked Outlook email {email_id} as read for user {user_id}")

            return {
                "message": "Email marked as read successfully",
                "email_id": email_id,
                "is_read": True,
                "labels": new_labels,
                "synced_to_provider": True,
                "provider": "microsoft"
            }

        else:
            # Gmail - use existing label-based approach
            # Pass connection_id to use the correct account's credentials
            service, _ = get_gmail_service(user_id, user_jwt, account_id=connection_id)

            if not service:
                raise ValueError("No active Google connection found for user.")

            # Mark as read = remove UNREAD label
            updated = service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'removeLabelIds': ['UNREAD']}
            ).execute()

            labels = updated.get('labelIds', [])

            logger.info(f"✅ Marked Gmail email {email_id} as read for user {user_id}")

            # Update in database
            auth_supabase.table('emails')\
                .update({
                    'is_read': True,
                    'labels': labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            return {
                "message": "Email marked as read successfully",
                "email_id": email_id,
                "is_read": True,
                "labels": labels,
                "synced_to_provider": True,
                "provider": "google"
            }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        raise ValueError(f"Failed to mark email as read: {str(e)}")
    except Exception as e:
        logger.error(f"Error marking email as read: {str(e)}")
        raise ValueError(f"Failed to mark as read: {str(e)}")


def mark_as_unread(
    user_id: str,
    user_jwt: str,
    email_id: str
) -> Dict[str, Any]:
    """
    Mark an email as unread.
    Two-way sync with database and provider (Gmail/Outlook).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID to mark as unread

    Returns:
        Dict with confirmation
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get provider info
    provider, connection_id, connection_data = _get_email_provider(user_id, email_id, user_jwt)

    try:
        if provider == 'microsoft':
            # Update Outlook via Graph API
            _update_outlook_read_status(email_id, is_read=False, connection_data=connection_data)

            # Get current labels and add UNREAD
            email_result = auth_supabase.table('emails')\
                .select('labels')\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .single()\
                .execute()

            current_labels = email_result.data.get('labels', []) if email_result.data else []
            new_labels = _update_labels_array(current_labels, add=['UNREAD'])

            # Update database
            auth_supabase.table('emails')\
                .update({
                    'is_read': False,
                    'labels': new_labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            logger.info(f"✅ Marked Outlook email {email_id} as unread for user {user_id}")

            return {
                "message": "Email marked as unread successfully",
                "email_id": email_id,
                "is_read": False,
                "labels": new_labels,
                "synced_to_provider": True,
                "provider": "microsoft"
            }

        else:
            # Gmail - use existing label-based approach
            # Pass connection_id to use the correct account's credentials
            service, _ = get_gmail_service(user_id, user_jwt, account_id=connection_id)

            if not service:
                raise ValueError("No active Google connection found for user.")

            # Mark as unread = add UNREAD label
            updated = service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'addLabelIds': ['UNREAD']}
            ).execute()

            labels = updated.get('labelIds', [])

            logger.info(f"✅ Marked Gmail email {email_id} as unread for user {user_id}")

            # Update in database
            auth_supabase.table('emails')\
                .update({
                    'is_read': False,
                    'labels': labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            return {
                "message": "Email marked as unread successfully",
                "email_id": email_id,
                "is_read": False,
                "labels": labels,
                "synced_to_provider": True,
                "provider": "google"
            }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        raise ValueError(f"Failed to mark email as unread: {str(e)}")
    except Exception as e:
        logger.error(f"Error marking email as unread: {str(e)}")
        raise ValueError(f"Failed to mark as unread: {str(e)}")


def mark_as_starred(
    user_id: str,
    user_jwt: str,
    email_id: str
) -> Dict[str, Any]:
    """
    Star an email.
    Two-way sync with database and provider (Gmail/Outlook).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID to star

    Returns:
        Dict with confirmation
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get provider info
    provider, connection_id, connection_data = _get_email_provider(user_id, email_id, user_jwt)

    try:
        if provider == 'microsoft':
            # Update Outlook via Graph API (flag)
            _update_outlook_flag_status(email_id, is_starred=True, connection_data=connection_data)

            # Get current labels and add STARRED
            email_result = auth_supabase.table('emails')\
                .select('labels')\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .single()\
                .execute()

            current_labels = email_result.data.get('labels', []) if email_result.data else []
            new_labels = _update_labels_array(current_labels, add=['STARRED'])

            # Update database
            auth_supabase.table('emails')\
                .update({
                    'is_starred': True,
                    'labels': new_labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            logger.info(f"✅ Starred Outlook email {email_id} for user {user_id}")

            return {
                "message": "Email starred successfully",
                "email_id": email_id,
                "is_starred": True,
                "labels": new_labels,
                "synced_to_provider": True,
                "provider": "microsoft"
            }

        else:
            # Gmail - use existing label-based approach
            # Pass connection_id to use the correct account's credentials
            service, _ = get_gmail_service(user_id, user_jwt, account_id=connection_id)

            if not service:
                raise ValueError("No active Google connection found for user.")

            # Star = add STARRED label
            updated = service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'addLabelIds': ['STARRED']}
            ).execute()

            labels = updated.get('labelIds', [])

            logger.info(f"✅ Starred Gmail email {email_id} for user {user_id}")

            # Update in database
            auth_supabase.table('emails')\
                .update({
                    'is_starred': True,
                    'labels': labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            return {
                "message": "Email starred successfully",
                "email_id": email_id,
                "is_starred": True,
                "labels": labels,
                "synced_to_provider": True,
                "provider": "google"
            }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        raise ValueError(f"Failed to star email: {str(e)}")
    except Exception as e:
        logger.error(f"Error starring email: {str(e)}")
        raise ValueError(f"Failed to star email: {str(e)}")


def unstar_email(
    user_id: str,
    user_jwt: str,
    email_id: str
) -> Dict[str, Any]:
    """
    Unstar an email.
    Two-way sync with database and provider (Gmail/Outlook).

    Args:
        user_id: User's ID
        user_jwt: User's Supabase JWT for authenticated requests
        email_id: Email message ID to unstar

    Returns:
        Dict with confirmation
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get provider info
    provider, connection_id, connection_data = _get_email_provider(user_id, email_id, user_jwt)

    try:
        if provider == 'microsoft':
            # Update Outlook via Graph API (unflag)
            _update_outlook_flag_status(email_id, is_starred=False, connection_data=connection_data)

            # Get current labels and remove STARRED
            email_result = auth_supabase.table('emails')\
                .select('labels')\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .single()\
                .execute()

            current_labels = email_result.data.get('labels', []) if email_result.data else []
            new_labels = _update_labels_array(current_labels, remove=['STARRED'])

            # Update database
            auth_supabase.table('emails')\
                .update({
                    'is_starred': False,
                    'labels': new_labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            logger.info(f"✅ Unstarred Outlook email {email_id} for user {user_id}")

            return {
                "message": "Email unstarred successfully",
                "email_id": email_id,
                "is_starred": False,
                "labels": new_labels,
                "synced_to_provider": True,
                "provider": "microsoft"
            }

        else:
            # Gmail - use existing label-based approach
            # Pass connection_id to use the correct account's credentials
            service, _ = get_gmail_service(user_id, user_jwt, account_id=connection_id)

            if not service:
                raise ValueError("No active Google connection found for user.")

            # Unstar = remove STARRED label
            updated = service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'removeLabelIds': ['STARRED']}
            ).execute()

            labels = updated.get('labelIds', [])

            logger.info(f"✅ Unstarred Gmail email {email_id} for user {user_id}")

            # Update in database
            auth_supabase.table('emails')\
                .update({
                    'is_starred': False,
                    'labels': labels
                    # normalized_labels auto-computed by generated column
                })\
                .eq('user_id', user_id)\
                .eq('external_id', email_id)\
                .execute()

            return {
                "message": "Email unstarred successfully",
                "email_id": email_id,
                "is_starred": False,
                "labels": labels,
                "synced_to_provider": True,
                "provider": "google"
            }

    except HttpError as e:
        logger.error(f"Gmail API error: {str(e)}")
        raise ValueError(f"Failed to unstar email: {str(e)}")
    except Exception as e:
        logger.error(f"Error unstarring email: {str(e)}")
        raise ValueError(f"Failed to unstar email: {str(e)}")
