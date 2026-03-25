"""Service for updating documents."""
from typing import Optional, List
import asyncio
from fastapi import HTTPException, status
from lib.supabase_client import get_authenticated_async_client
from api.services.notifications.file_edits import emit_document_edited_notification
import logging

logger = logging.getLogger(__name__)

# Minimum content-length change required to trigger a snapshot.
MIN_DIFF_CHARS = 100


def _should_snapshot(
    document_type: Optional[str],
    old_content: str,
    new_content: Optional[str],
    force: bool = False,
) -> bool:
    """Decide whether a version snapshot should be attempted.

    Cheap deterministic checks only — no DB calls.  The time-interval gate
    lives in the ``insert_document_version_snapshot`` RPC so it is serialised
    under the advisory lock.

    When *force* is True every check except "is it a note?" is bypassed.
    This is used by the restore flow to guarantee the pre-restore state is
    always captured.
    """
    # Always require the document to be a note.
    if document_type and document_type != "note":
        logger.debug("[version] skip: document type is '%s', not 'note'", document_type)
        return False

    if force:
        logger.info("[version] force_snapshot requested — bypassing diff checks")
        return True

    if new_content is None:
        logger.debug("[version] skip: new_content is None (no content change)")
        return False

    # Content must actually differ.
    if old_content == new_content:
        logger.debug("[version] skip: content unchanged")
        return False

    # First non-empty save — always capture as Version 1 regardless of size.
    if not old_content.strip():
        logger.info("[version] first non-empty save — will snapshot")
        return True

    # Require a minimum magnitude of change (avoids snapshotting typo fixes).
    diff_size = abs(len(new_content) - len(old_content))
    if diff_size < MIN_DIFF_CHARS:
        logger.debug(
            "[version] skip: diff size %d < threshold %d",
            diff_size,
            MIN_DIFF_CHARS,
        )
        return False

    return True


async def _snapshot_version_async(
    user_jwt: str,
    document_id: str,
    old_title: str,
    old_content: str,
    user_id: str,
    force: bool = False,
) -> bool:
    """Snapshot previous document content in the background.

    The interval check is handled atomically inside the RPC function under
    an advisory lock — no Python-side time check needed.

    When *force* is True the RPC is told to bypass the interval gate (used
    by restore to guarantee reversibility).

    Failures are intentionally non-blocking to keep save latency stable.
    """
    try:
        auth_supabase = await get_authenticated_async_client(user_jwt)

        insert_result = await (
            auth_supabase.rpc(
                "insert_document_version_snapshot",
                {
                    "p_document_id": document_id,
                    "p_title": old_title,
                    "p_content": old_content,
                    "p_created_by": user_id,
                    "p_force": force,
                },
            )
            .execute()
        )

        if not insert_result.data:
            # RPC returned no rows — interval gate rejected the snapshot.
            logger.info("[version] %s: skipped by RPC (interval not met)", document_id)
            return False

        version_number = insert_result.data[0].get("version_number")
        logger.info("[version] %s: saved v%s%s", document_id, version_number, " (forced)" if force else "")

        # Keep at most 50 versions per document (best effort).
        try:
            cutoff_result = await (
                auth_supabase.table("document_versions")
                .select("id")
                .eq("document_id", document_id)
                .order("version_number", desc=True)
                .range(50, 150)
                .execute()
            )
            if cutoff_result.data:
                old_ids = [row["id"] for row in cutoff_result.data]
                await (
                    auth_supabase.table("document_versions")
                    .delete()
                    .in_("id", old_ids)
                    .execute()
                )
                logger.info("[version] %s: pruned %d old versions", document_id, len(old_ids))
        except Exception as prune_exc:
            logger.warning(
                "[version] %s: prune skipped — %s: %s",
                document_id,
                type(prune_exc).__name__,
                prune_exc,
            )

        return True
    except Exception as exc:
        logger.error(
            "[version] %s: FAILED — %s: %s",
            document_id,
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return False


async def _process_document_change_async(
    *,
    user_jwt: str,
    document_id: str,
    old_title: str,
    old_content: str,
    new_title: str,
    user_id: str,
    workspace_id: Optional[str],
    owner_id: Optional[str],
    file_id: Optional[str],
    should_snapshot: bool,
    title_changed: bool,
    force_snapshot: bool = False,
) -> None:
    """Run non-blocking versioning and notification side effects for a save."""
    if should_snapshot:
        await _snapshot_version_async(
            user_jwt=user_jwt,
            document_id=document_id,
            old_title=old_title,
            old_content=old_content,
            user_id=user_id,
            force=force_snapshot,
        )

    if not should_snapshot and not title_changed:
        return

    try:
        await emit_document_edited_notification(
            document_id=document_id,
            document_title=new_title,
            editor_user_id=user_id,
            workspace_id=workspace_id,
            owner_id=owner_id,
            file_id=file_id,
        )
    except Exception as exc:
        logger.warning(
            "[file_edited] %s: notification skipped — %s: %s",
            document_id,
            type(exc).__name__,
            exc,
        )


async def update_document(
    user_id: str,
    user_jwt: str,
    document_id: str,
    title: Optional[str] = None,
    content: Optional[str] = None,
    icon: Optional[str] = None,
    cover_image: Optional[str] = None,
    parent_id: Optional[str] = None,
    position: Optional[int] = None,
    tags: Optional[List[str]] = None,
    parent_id_explicitly_set: bool = False,
    expected_updated_at: Optional[str] = None,
    force_snapshot: bool = False,
) -> dict:
    """
    Update an existing document.

    Authorization is handled by RLS - owner or workspace member can update.

    Args:
        user_id: ID of the user performing the update
        user_jwt: User's Supabase JWT for authenticated requests
        document_id: Document ID to update
        title: New title (optional)
        content: New content (optional)
        icon: New icon (optional)
        cover_image: New cover image (optional)
        parent_id: New parent ID (optional, can be None to move to root)
        position: New position (optional)
        tags: New tags list (optional)
        parent_id_explicitly_set: If True, parent_id will be updated even if None
                                  (used to move documents to root)
        expected_updated_at: Optional optimistic-lock timestamp from client.
        force_snapshot: If True, always create a version snapshot of the current
                        state before applying this update (bypasses interval and
                        diff-size checks).  Used by restore_version to guarantee
                        the pre-restore state is preserved.

    Returns:
        The updated document record
    """
    auth_supabase = await get_authenticated_async_client(user_jwt)

    try:
        # Build update data
        update_data = {}

        if title is not None:
            update_data["title"] = title
        if content is not None:
            update_data["content"] = content
        if icon is not None:
            update_data["icon"] = icon
        if cover_image is not None:
            update_data["cover_image"] = cover_image
        # Handle parent_id specially - if explicitly set, include it even if None
        if parent_id_explicitly_set:
            update_data["parent_id"] = parent_id
        elif parent_id is not None:
            update_data["parent_id"] = parent_id
        if position is not None:
            update_data["position"] = position
        if tags is not None:
            update_data["tags"] = tags

        if not update_data:
            raise ValueError("No fields to update")

        # Read current state once for optimistic locking and version snapshot inputs.
        current_result = await (
            auth_supabase.table("documents")
            .select("title, content, type, updated_at, workspace_id, user_id, file_id")
            .eq("id", document_id)
            .limit(1)
            .execute()
        )
        if not current_result.data:
            raise Exception("Document not found or access denied")

        current_doc = current_result.data[0]
        old_title = current_doc.get("title") or ""
        old_content = current_doc.get("content") or ""
        doc_type = current_doc.get("type")
        workspace_id = current_doc.get("workspace_id")
        owner_id = current_doc.get("user_id")
        file_id = current_doc.get("file_id")

        update_query = (
            auth_supabase.table("documents")
            .update(update_data)
            .eq("id", document_id)
        )
        # Only enforce optimistic locking when the client explicitly provides
        # the last-seen updated_at token.
        optimistic_ts = expected_updated_at
        if optimistic_ts:
            update_query = update_query.eq("updated_at", optimistic_ts)

        # Update document - RLS handles authorization (owner or workspace member)
        result = await update_query.execute()

        if not result.data:
            exists_result = await (
                auth_supabase.table("documents")
                .select("id")
                .eq("id", document_id)
                .limit(1)
                .execute()
            )
            if exists_result.data:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Document changed in another session. Refresh and retry.",
                )
            raise Exception("Document not found or access denied")

        logger.info(f"Updated document {document_id} by user {user_id}")

        # Re-embed if content or title changed (fire-and-forget)
        new_title = title if title is not None else old_title
        new_content = content if content is not None else old_content
        if title is not None or content is not None:
            from lib.embed_hooks import embed_document
            embed_document(document_id, new_title, new_content)

        should_snapshot = _should_snapshot(doc_type, old_content, content, force=force_snapshot)
        title_changed = (
            doc_type == "note"
            and title is not None
            and title != old_title
        )

        if should_snapshot or title_changed:
            asyncio.create_task(
                _process_document_change_async(
                    user_jwt=user_jwt,
                    document_id=document_id,
                    old_title=old_title,
                    old_content=old_content,
                    new_title=new_title,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    owner_id=owner_id,
                    file_id=file_id,
                    should_snapshot=should_snapshot,
                    title_changed=title_changed,
                    force_snapshot=force_snapshot,
                )
            )

        # Fetch the complete document with file data joined
        # (UPDATE doesn't support joins in response)
        full_doc = await (
            auth_supabase.table("documents")
            .select("*, file:files(*)")
            .eq("id", document_id)
            .execute()
        )

        if full_doc.data:
            return full_doc.data[0]
        return result.data[0]

    except Exception as e:
        logger.error(f"Error updating document {document_id}: {str(e)}")
        raise
