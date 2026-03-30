"""
Ultramemory async client for 10xApp Core.

Wraps the Ultramemory REST API for use by chat tools and auto-ingest hooks.
Each user gets their own session namespace for memory isolation.
"""

import logging
from typing import Any, Dict, List, Optional

import httpx

from api.config import settings

logger = logging.getLogger(__name__)

# Reuse a single async client across requests
_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            base_url=settings.ultramemory_url,
            timeout=30.0,
        )
    return _client


def _user_session(user_id: str) -> str:
    """Map a 10xApp user ID to an Ultramemory session name."""
    return f"10x-{user_id[:12]}"


# ─── Core Operations ────────────────────────────────────────────


async def ingest(
    user_id: str,
    content: str,
    category: str = "conversation",
    session: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest a memory into Ultramemory.

    Args:
        user_id: 10xApp user UUID
        content: Text content to remember
        category: Memory category (conversation, preference, fact, email, etc.)
        session: Override session name (defaults to user-scoped)

    Returns:
        API response dict with memory count
    """
    client = _get_client()
    payload = {
        "content": content,
        "session": session or _user_session(user_id),
        "metadata": {"source": "10xapp", "category": category},
    }
    try:
        resp = await client.post("/api/ingest", json=payload)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        logger.error(f"Ultramemory ingest failed: {e}")
        return {"error": str(e), "count": 0}


async def search(
    user_id: str,
    query: str,
    top_k: int = 5,
    min_score: float = 0.55,
    session: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Semantic search across a user's memories.

    Args:
        user_id: 10xApp user UUID
        query: Natural language search query
        top_k: Number of results to return
        min_score: Minimum similarity threshold
        session: Override session name

    Returns:
        List of memory dicts with content, score, created_at
    """
    client = _get_client()
    payload = {
        "query": query,
        "top_k": top_k,
        "min_score": min_score,
        "session": session or _user_session(user_id),
    }
    try:
        resp = await client.post("/api/search", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except httpx.HTTPError as e:
        logger.error(f"Ultramemory search failed: {e}")
        return []


async def recall(
    user_id: str,
    query: str,
    session: Optional[str] = None,
) -> str:
    """
    High-level recall: search + format results into a readable context block.

    Returns a formatted string suitable for injection into system prompts.
    """
    results = await search(user_id, query, top_k=5, session=session)
    if not results:
        return ""

    lines = ["## Relevant Memories\n"]
    for r in results:
        score = r.get("score", 0)
        content = r.get("content", "")
        created = r.get("created_at", "")[:10]
        lines.append(f"- [{score:.0%}] ({created}) {content}")
    return "\n".join(lines)


async def startup_context(
    user_id: str,
    session: Optional[str] = None,
) -> str:
    """
    Get startup context for a user — recent memories + profile summary.

    Useful for pre-loading the system prompt with relevant context
    before the user sends their first message.
    """
    client = _get_client()
    payload = {
        "session": session or _user_session(user_id),
    }
    try:
        resp = await client.post("/api/startup-context", json=payload)
        resp.raise_for_status()
        data = resp.json()
        context_parts = []
        if data.get("profile"):
            context_parts.append(f"## User Profile\n{data['profile']}")
        if data.get("recent_memories"):
            context_parts.append("## Recent Context")
            for m in data["recent_memories"][:5]:
                context_parts.append(f"- {m.get('content', '')}")
        return "\n\n".join(context_parts)
    except httpx.HTTPError as e:
        logger.error(f"Ultramemory startup-context failed: {e}")
        return ""


async def health() -> bool:
    """Check if Ultramemory is reachable."""
    client = _get_client()
    try:
        resp = await client.get("/api/health")
        return resp.status_code == 200
    except httpx.HTTPError:
        return False
