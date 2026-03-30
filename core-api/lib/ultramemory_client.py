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

# Reuse a single async client across requests (created lazily)
_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    """Get or create the shared async HTTP client."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            base_url=settings.ultramemory_url,
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
    return _client


async def close():
    """Close the HTTP client. Call from app shutdown/lifespan."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


def _user_session(user_id: str) -> str:
    """Map a 10xApp user ID to an Ultramemory session name.

    Uses the full user UUID — no truncation — to avoid collisions.
    """
    return f"10x-{user_id}"


# ─── Core Operations ────────────────────────────────────────────


async def ingest(
    user_id: str,
    content: str,
    category: str = "conversation",
    session: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest a memory into Ultramemory.

    Returns:
        API response dict with memory count, or {"error": ...} on failure.
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
    except Exception as e:
        logger.error(f"Ultramemory ingest failed: {e}")
        return {"error": str(e), "count": 0}


async def search(
    user_id: str,
    query: str,
    top_k: int = 5,
    min_score: float = 0.55,
    session: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Semantic search across a user's memories.

    Returns:
        List of memory dicts on success, None on backend failure.
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
    except Exception as e:
        logger.error(f"Ultramemory search failed: {e}")
        return None  # Distinct from empty results


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
        try:
            score = float(r.get("score", 0))
        except (ValueError, TypeError):
            score = 0.0
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
    except Exception as e:
        logger.error(f"Ultramemory startup-context failed: {e}")
        return ""


async def health() -> bool:
    """Check if Ultramemory is reachable."""
    client = _get_client()
    try:
        resp = await client.get("/api/health")
        return resp.status_code == 200
    except Exception:
        return False
