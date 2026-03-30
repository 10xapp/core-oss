"""
Memory tools powered by Ultramemory.

Replaces the stub update_memory tool with three real tools:
- remember: Save information to long-term memory
- recall: Search memory for relevant context
- search_memory: Detailed semantic search with scores
"""

import logging
from typing import Dict

from lib.tools.base import ToolCategory, ToolContext, ToolResult, success, error
from lib.tools.registry import tool

logger = logging.getLogger(__name__)


def _get_memory():
    """Lazy import to avoid failure when Ultramemory is not configured."""
    from lib import ultramemory_client as memory
    return memory


@tool(
    name="remember",
    description=(
        "Save important information to long-term memory. Use this when the user "
        "shares preferences, facts about themselves, decisions, or asks you to "
        "remember something. Memories persist across conversations."
    ),
    params={
        "content": "The information to remember (be specific and self-contained)",
        "category": "Category: 'preference', 'fact', 'decision', 'todo', 'person', 'event'",
    },
    required=["content"],
    category=ToolCategory.MEMORY,
    status="Saving to memory...",
)
async def remember(args: Dict, ctx: ToolContext) -> ToolResult:
    content = args.get("content", "")
    category = args.get("category", "fact")

    if not content:
        return error("Nothing to remember — content is empty.")

    try:
        memory = _get_memory()
        result = await memory.ingest(
            user_id=ctx.user_id,
            content=content,
            category=category,
        )
    except Exception as e:
        logger.warning(f"Memory ingest failed: {e}")
        return error("Memory service is temporarily unavailable. Please try again later.")

    if result.get("error"):
        return error(f"Failed to save memory: {result['error']}")

    count = result.get("count", 0)
    return success(
        {"saved": True, "memories_created": count, "content": content},
        f"Remembered: {content[:100]}{'...' if len(content) > 100 else ''}",
    )


@tool(
    name="recall",
    description=(
        "Search long-term memory for relevant information. Use this before answering "
        "questions about the user's preferences, past conversations, or personal details. "
        "Returns the most relevant memories ranked by similarity."
    ),
    params={
        "query": "What to search for (natural language — be descriptive)",
    },
    required=["query"],
    category=ToolCategory.MEMORY,
    status="Searching memory...",
)
async def recall(args: Dict, ctx: ToolContext) -> ToolResult:
    query = args.get("query", "")

    if not query:
        return error("No search query provided.")

    try:
        memory = _get_memory()
        results = await memory.search(
            user_id=ctx.user_id,
            query=query,
            top_k=5,
        )
    except Exception as e:
        logger.warning(f"Memory search failed: {e}")
        return error("Memory service is temporarily unavailable.")

    if results is None:
        return error("Memory search failed — backend returned an error.")

    if not results:
        return success(
            {"results": [], "count": 0},
            "No relevant memories found.",
        )

    # Format for the LLM
    formatted = []
    for r in results:
        try:
            score = float(r.get("score", 0))
        except (ValueError, TypeError):
            score = 0.0
        formatted.append({
            "content": r.get("content", ""),
            "relevance": f"{score:.0%}",
            "date": r.get("created_at", "")[:10],
        })

    return success(
        {"results": formatted, "count": len(formatted)},
        f"Found {len(formatted)} relevant memories.",
    )


@tool(
    name="search_memory",
    description=(
        "Advanced memory search with filters. Returns detailed results including "
        "similarity scores and metadata. Use for specific lookups when recall "
        "isn't precise enough."
    ),
    params={
        "query": "Search query (natural language)",
        "max_results": "Maximum results to return (default 10)",
    },
    required=["query"],
    category=ToolCategory.MEMORY,
    status="Searching memory...",
)
async def search_memory(args: Dict, ctx: ToolContext) -> ToolResult:
    query = args.get("query", "")
    try:
        top_k = max(1, min(50, int(args.get("max_results", 10))))
    except (ValueError, TypeError):
        top_k = 10

    if not query:
        return error("No search query provided.")

    try:
        memory = _get_memory()
        results = await memory.search(
            user_id=ctx.user_id,
            query=query,
            top_k=top_k,
            min_score=0.45,  # Lower threshold for broader results
        )
    except Exception as e:
        logger.warning(f"Memory search failed: {e}")
        return error("Memory service is temporarily unavailable.")

    if results is None:
        return error("Memory search failed — backend returned an error.")

    if not results:
        return success({"results": [], "count": 0}, "No memories found.")

    formatted = []
    for r in results:
        try:
            score = float(r.get("score", 0))
        except (ValueError, TypeError):
            score = 0.0
        entry = {
            "content": r.get("content", ""),
            "score": score,
            "date": r.get("created_at", ""),
            "entities": r.get("entities", []),
        }
        formatted.append(entry)

    return success(
        {"results": formatted, "count": len(formatted), "query": query},
        f"Found {len(formatted)} memories matching '{query[:50]}'.",
    )
