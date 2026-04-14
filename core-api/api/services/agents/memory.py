"""
Agent memory service — CRUD and vector search for agent memories and workspace knowledge base.
"""
from typing import List, Dict, Any, Optional
import logging

from lib.supabase_client import get_authenticated_async_client, get_service_role_client

logger = logging.getLogger(__name__)


# =============================================================================
# AGENT MEMORIES
# =============================================================================


async def store_memory(
    agent_id: str,
    workspace_id: str,
    memory_type: str,
    content: str,
    user_jwt: str,
    summary: Optional[str] = None,
    source_type: Optional[str] = None,
    source_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    is_shared: bool = False,
    decay_factor: float = 0.95,
) -> Dict[str, Any]:
    """Store a new memory for an agent."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        row: Dict[str, Any] = {
            "agent_id": agent_id,
            "workspace_id": workspace_id,
            "memory_type": memory_type,
            "content": content,
            "is_shared": is_shared,
            "decay_factor": decay_factor,
        }
        if summary:
            row["summary"] = summary
        if source_type:
            row["source_type"] = source_type
        if source_id:
            row["source_id"] = source_id
        if tags:
            row["tags"] = tags

        # Generate embedding if possible
        embedding = await _generate_embedding(content)
        if embedding:
            row["embedding"] = embedding

        result = await (
            supabase.table("agent_memories")
            .insert(row)
            .execute()
        )
        memory = result.data[0]
        logger.info(f"Stored {memory_type} memory ({memory['id']}) for agent {agent_id}")
        return memory
    except Exception as e:
        logger.error(f"Error storing memory for agent {agent_id}: {e}")
        raise


async def search_memories(
    agent_id: str,
    workspace_id: str,
    query: str,
    user_jwt: str,
    match_count: int = 10,
    similarity_threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    """Search agent memories using vector similarity."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        embedding = await _generate_embedding(query)
        if not embedding:
            logger.warning("Could not generate embedding for search query, falling back to text search")
            return await get_memories(agent_id, user_jwt)

        result = await supabase.rpc(
            "search_agent_memories",
            {
                "p_agent_id": agent_id,
                "p_workspace_id": workspace_id,
                "p_embedding": embedding,
                "p_match_count": match_count,
                "p_similarity_threshold": similarity_threshold,
            },
        ).execute()
        memories = result.data or []
        logger.info(f"Found {len(memories)} matching memories for agent {agent_id}")
        return memories
    except Exception as e:
        logger.error(f"Error searching memories for agent {agent_id}: {e}")
        raise


async def get_memories(
    agent_id: str,
    user_jwt: str,
    memory_type: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Get memories for an agent, optionally filtered by type."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        query = (
            supabase.table("agent_memories")
            .select("*")
            .eq("agent_id", agent_id)
            .is_("expired_at", "null")
            .order("created_at", desc=True)
            .limit(limit)
        )
        if memory_type:
            query = query.eq("memory_type", memory_type)

        result = await query.execute()
        memories = result.data or []
        logger.info(f"Retrieved {len(memories)} memories for agent {agent_id}")
        return memories
    except Exception as e:
        logger.error(f"Error getting memories for agent {agent_id}: {e}")
        raise


async def update_memory(
    memory_id: str,
    updates: Dict[str, Any],
    user_jwt: str,
) -> Dict[str, Any]:
    """Update an existing memory."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        # Re-generate embedding if content changed
        if "content" in updates:
            embedding = await _generate_embedding(updates["content"])
            if embedding:
                updates["embedding"] = embedding

        result = await (
            supabase.table("agent_memories")
            .update(updates)
            .eq("id", memory_id)
            .execute()
        )
        memory = result.data[0]
        logger.info(f"Updated memory {memory_id}")
        return memory
    except Exception as e:
        logger.error(f"Error updating memory {memory_id}: {e}")
        raise


async def delete_memory(memory_id: str, user_jwt: str) -> None:
    """Delete a memory."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        await (
            supabase.table("agent_memories")
            .delete()
            .eq("id", memory_id)
            .execute()
        )
        logger.info(f"Deleted memory {memory_id}")
    except Exception as e:
        logger.error(f"Error deleting memory {memory_id}: {e}")
        raise


async def access_memory(memory_id: str, user_jwt: str) -> Dict[str, Any]:
    """Record an access to a memory (bumps access_count and last_accessed_at)."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        # Fetch current to increment
        current = await (
            supabase.table("agent_memories")
            .select("access_count")
            .eq("id", memory_id)
            .single()
            .execute()
        )
        new_count = (current.data.get("access_count", 0) or 0) + 1

        result = await (
            supabase.table("agent_memories")
            .update({
                "access_count": new_count,
                "last_accessed_at": "now()",
            })
            .eq("id", memory_id)
            .execute()
        )
        return result.data[0]
    except Exception as e:
        logger.error(f"Error accessing memory {memory_id}: {e}")
        raise


# =============================================================================
# KNOWLEDGE BASE
# =============================================================================


async def add_knowledge(
    workspace_id: str,
    category: str,
    key: str,
    value: str,
    user_jwt: str,
    source_agent_id: Optional[str] = None,
    confidence: float = 1.0,
) -> Dict[str, Any]:
    """Add an entry to the workspace knowledge base."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        row: Dict[str, Any] = {
            "workspace_id": workspace_id,
            "category": category,
            "key": key,
            "value": value,
            "confidence": confidence,
        }
        if source_agent_id:
            row["source_agent_id"] = source_agent_id

        embedding = await _generate_embedding(f"{category}: {key} — {value}")
        if embedding:
            row["embedding"] = embedding

        result = await (
            supabase.table("agent_knowledge_base")
            .insert(row)
            .execute()
        )
        entry = result.data[0]
        logger.info(f"Added knowledge '{category}/{key}' in workspace {workspace_id}")
        return entry
    except Exception as e:
        logger.error(f"Error adding knowledge: {e}")
        raise


async def search_knowledge(
    workspace_id: str,
    query: str,
    user_jwt: str,
    match_count: int = 10,
    similarity_threshold: float = 0.5,
) -> List[Dict[str, Any]]:
    """Search the workspace knowledge base using vector similarity."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        embedding = await _generate_embedding(query)
        if not embedding:
            logger.warning("Could not generate embedding, falling back to list")
            return await get_knowledge(workspace_id, user_jwt)

        result = await supabase.rpc(
            "search_knowledge_base",
            {
                "p_workspace_id": workspace_id,
                "p_embedding": embedding,
                "p_match_count": match_count,
                "p_similarity_threshold": similarity_threshold,
            },
        ).execute()
        entries = result.data or []
        logger.info(f"Found {len(entries)} matching knowledge entries in workspace {workspace_id}")
        return entries
    except Exception as e:
        logger.error(f"Error searching knowledge in workspace {workspace_id}: {e}")
        raise


async def get_knowledge(
    workspace_id: str,
    user_jwt: str,
    category: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Get knowledge base entries for a workspace."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        query = (
            supabase.table("agent_knowledge_base")
            .select("*")
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(limit)
        )
        if category:
            query = query.eq("category", category)

        result = await query.execute()
        entries = result.data or []
        logger.info(f"Retrieved {len(entries)} knowledge entries for workspace {workspace_id}")
        return entries
    except Exception as e:
        logger.error(f"Error getting knowledge for workspace {workspace_id}: {e}")
        raise


async def verify_knowledge(
    knowledge_id: str,
    verified_by: str,
    user_jwt: str,
) -> Dict[str, Any]:
    """Mark a knowledge entry as verified by a human."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        result = await (
            supabase.table("agent_knowledge_base")
            .update({
                "verified_by": verified_by,
                "verified_at": "now()",
            })
            .eq("id", knowledge_id)
            .execute()
        )
        entry = result.data[0]
        logger.info(f"Verified knowledge entry {knowledge_id}")
        return entry
    except Exception as e:
        logger.error(f"Error verifying knowledge {knowledge_id}: {e}")
        raise


async def delete_knowledge(knowledge_id: str, user_jwt: str) -> None:
    """Delete a knowledge base entry."""
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        await (
            supabase.table("agent_knowledge_base")
            .delete()
            .eq("id", knowledge_id)
            .execute()
        )
        logger.info(f"Deleted knowledge entry {knowledge_id}")
    except Exception as e:
        logger.error(f"Error deleting knowledge {knowledge_id}: {e}")
        raise


# =============================================================================
# HELPERS
# =============================================================================


async def _generate_embedding(text: str) -> Optional[List[float]]:
    """Generate an embedding vector for the given text.

    Uses OpenAI's text-embedding-3-small model. Returns None if the API key
    is not configured or the call fails, so callers can gracefully degrade.
    """
    try:
        import os
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return None

        import httpx
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": "text-embedding-3-small",
                    "input": text[:8000],
                },
                timeout=10.0,
            )
            response.raise_for_status()
            return response.json()["data"][0]["embedding"]
    except Exception as e:
        logger.warning(f"Failed to generate embedding: {e}")
        return None
