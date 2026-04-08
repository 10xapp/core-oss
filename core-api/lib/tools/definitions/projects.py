"""Project tools: list_project_boards, get_project_board, get_project_issue, create_project_issue."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

from lib.tools.base import ToolCategory, ToolContext, ToolResult, error, staged_result, success
from lib.tools.registry import tool

logger = logging.getLogger(__name__)

_MAX_BOARD_ISSUES = 200
_MAX_BOARD_LIST = 100
_DESCRIPTION_PREVIEW_LEN = 280


def _clamp_limit(raw_value: Any, default: int, maximum: int) -> int:
    """Parse an integer limit and clamp it to a safe range."""
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, maximum))


def _coerce_bool(raw_value: Any, default: bool = False) -> bool:
    """Parse booleans passed as actual booleans or common string forms."""
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        normalized = raw_value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    if raw_value is None:
        return default
    return bool(raw_value)


def _truncate_text(value: str, limit: int = _DESCRIPTION_PREVIEW_LEN) -> str:
    """Trim long text so board overviews do not blow up the context window."""
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _format_reference(board_key: str | None, issue_number: Any) -> str | None:
    """Build a user-facing issue reference like CORE-12 when possible."""
    if issue_number is None:
        return None
    if board_key:
        return f"{board_key}-{issue_number}"
    return f"#{issue_number}"


async def _get_member_map(workspace_id: str, user_jwt: str) -> Dict[str, Dict[str, Any]]:
    """Resolve workspace member names so assignee data is useful to the model."""
    from api.services.workspaces import get_workspace_members

    try:
        members = await get_workspace_members(workspace_id, user_jwt)
    except Exception as exc:
        logger.warning(
            "[CHAT] Failed to enrich project assignees for workspace %s: %s",
            workspace_id,
            exc,
        )
        return {}

    return {
        member["user_id"]: {
            "user_id": member.get("user_id"),
            "name": member.get("name"),
            "email": member.get("email"),
            "avatar_url": member.get("avatar_url"),
        }
        for member in members
        if member.get("user_id")
    }


def _simplify_labels(labels: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    """Return compact label objects for LLM consumption."""
    return [
        {
            "id": label.get("id"),
            "name": label.get("name"),
            "color": label.get("color"),
        }
        for label in (labels or [])
    ]


def _simplify_assignees(
    assignees: List[Dict[str, Any]] | None,
    member_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach names and emails to issue assignees when available."""
    simplified: List[Dict[str, Any]] = []
    for assignee in assignees or []:
        user_id = assignee.get("user_id")
        member = member_map.get(user_id, {})
        simplified.append(
            {
                "user_id": user_id,
                "name": member.get("name"),
                "email": member.get("email"),
                "avatar_url": member.get("avatar_url"),
            }
        )
    return simplified


def _format_issue_summary(
    issue: Dict[str, Any],
    state_lookup: Dict[str, Dict[str, Any]],
    member_map: Dict[str, Dict[str, Any]],
    board_key: str | None,
) -> Dict[str, Any]:
    """Create a compact issue summary for board-level reads."""
    state = state_lookup.get(issue.get("state_id"), {})
    description = issue.get("description") or ""

    summary: Dict[str, Any] = {
        "id": issue.get("id"),
        "board_id": issue.get("board_id"),
        "state_id": issue.get("state_id"),
        "state_name": state.get("name"),
        "is_done": bool(state.get("is_done")) or bool(issue.get("completed_at")),
        "number": issue.get("number"),
        "reference": _format_reference(board_key, issue.get("number")),
        "title": issue.get("title"),
        "priority": issue.get("priority", 0),
        "position": issue.get("position", 0),
        "due_at": issue.get("due_at"),
        "completed_at": issue.get("completed_at"),
        "labels": _simplify_labels(issue.get("label_objects")),
        "assignees": _simplify_assignees(issue.get("assignees"), member_map),
        "image_count": len(issue.get("image_urls") or issue.get("image_r2_keys") or []),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
    }
    if description:
        summary["description_preview"] = _truncate_text(description)
        summary["description_truncated"] = len(description) > _DESCRIPTION_PREVIEW_LEN
    return summary


def _format_board_summary(board: Dict[str, Any], issue_count: int, open_issue_count: int) -> Dict[str, Any]:
    """Create a compact board summary."""
    summary: Dict[str, Any] = {
        "id": board.get("id"),
        "workspace_id": board.get("workspace_id"),
        "workspace_app_id": board.get("workspace_app_id"),
        "name": board.get("name"),
        "key": board.get("key"),
        "icon": board.get("icon"),
        "color": board.get("color"),
        "position": board.get("position"),
        "next_issue_number": board.get("next_issue_number"),
        "issue_count": issue_count,
        "open_issue_count": open_issue_count,
        "created_at": board.get("created_at"),
        "updated_at": board.get("updated_at"),
        "url_path": f"/workspace/{board.get('workspace_id')}/projects/{board.get('id')}",
    }
    if board.get("description"):
        summary["description"] = board["description"]
    return summary


def _parse_issue_priority(raw_value: Any) -> tuple[int | None, str | None]:
    """Validate an optional issue priority and return a user-facing error when invalid."""
    if raw_value is None:
        return None, None

    try:
        priority = int(raw_value)
    except (TypeError, ValueError):
        return None, "priority must be an integer between 0 and 4"

    if not 0 <= priority <= 4:
        return None, "priority must be an integer between 0 and 4"

    return priority, None


def _parse_issue_due_at(raw_value: Any) -> tuple[str | None, str | None]:
    """Validate and normalize an optional issue due date."""
    if raw_value is None:
        return None, None

    try:
        due_at = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None, "due_at must be a valid ISO 8601 datetime"

    return due_at.isoformat(), None


def _resolve_target_state(
    states: List[Dict[str, Any]],
    requested_state_id: Any,
) -> tuple[Dict[str, Any] | None, str | None]:
    """Pick the requested state or the first non-done state for staged issue creation."""
    if requested_state_id:
        target_state = next((state for state in states if state.get("id") == requested_state_id), None)
        if not target_state:
            return None, "Project state not found on this board"
        return target_state, None

    target_state = next((state for state in states if not state.get("is_done")), None)
    if not target_state:
        return None, "Project board has no non-complete state available"
    return target_state, None


@tool(
    name="create_project_issue",
    description=(
        "Prepare a new project issue/card on a board. Use this when the user asks to add, create, "
        "or file a task/card/issue on a project board."
    ),
    params={
        "board_id": "Project board ID where the card should be created",
        "title": "Issue/card title",
        "state_id": "Optional target state/column ID. If omitted, the first non-done state is used.",
        "description": "Optional issue description",
        "priority": "Optional priority from 0-4 (0 none, 1 urgent, 2 high, 3 medium, 4 low)",
        "due_at": "Optional due date/time in ISO 8601 format",
    },
    required=["board_id", "title"],
    category=ToolCategory.PROJECTS,
    staged=True,
    status="Preparing project issue..."
)
async def create_project_issue(args: Dict, ctx: ToolContext) -> ToolResult:
    from api.services.projects import get_board_by_id, get_states

    board_id = args.get("board_id")
    title = (args.get("title") or "").strip()
    requested_state_id = args.get("state_id")

    if not board_id:
        return error("board_id is required")
    if not title:
        return error("title is required")

    logger.info(
        "[CHAT] User %s staging project issue on board %s (state=%s)",
        ctx.user_id,
        board_id,
        requested_state_id,
    )

    board = await get_board_by_id(ctx.user_jwt, board_id)
    if not board:
        return error("Project board not found")

    if ctx.workspace_ids and board.get("workspace_id") not in ctx.workspace_ids:
        return error("Project board is outside the current workspace scope")

    states = await get_states(ctx.user_jwt, board_id)
    if not states:
        return error("Project board has no states")

    target_state, state_error = _resolve_target_state(states, requested_state_id)
    if state_error:
        return error(state_error)

    staged_args: Dict[str, Any] = {
        "board_id": board_id,
        "board_name": board.get("name"),
        "state_id": target_state.get("id"),
        "state_name": target_state.get("name"),
        "title": title,
    }

    if args.get("description") is not None:
        staged_args["description"] = args.get("description")
    priority, priority_error = _parse_issue_priority(args.get("priority"))
    if priority_error:
        return error(priority_error)
    if priority is not None:
        staged_args["priority"] = priority
    due_at, due_at_error = _parse_issue_due_at(args.get("due_at"))
    if due_at_error:
        return error(due_at_error)
    if due_at is not None:
        staged_args["due_at"] = due_at

    return staged_result(
        "create_project_issue",
        staged_args,
        f"Create issue in {board.get('name', 'project board')}: {title}",
    )


@tool(
    name="list_project_boards",
    description=(
        "List project boards across the current workspace scope. Use this when the user asks "
        "which boards exist or when you need to identify a board before reading it."
    ),
    params={
        "limit": "Maximum number of boards to return (default 50, max 100)",
    },
    category=ToolCategory.PROJECTS,
    status="Loading project boards..."
)
async def list_project_boards(args: Dict, ctx: ToolContext) -> ToolResult:
    from lib.supabase_client import get_authenticated_async_client

    limit = _clamp_limit(args.get("limit"), default=50, maximum=_MAX_BOARD_LIST)
    logger.info("[CHAT] User %s listing project boards (limit=%s)", ctx.user_id, limit)

    supabase = await get_authenticated_async_client(ctx.user_jwt)
    query = (
        supabase.table("project_boards")
        .select(
            "id, workspace_id, workspace_app_id, name, description, key, icon, color, "
            "position, next_issue_number, created_at, updated_at"
        )
        .order("position")
        .limit(limit)
    )

    if ctx.workspace_ids:
        query = query.in_("workspace_id", ctx.workspace_ids)

    result = await query.execute()
    boards = result.data or []

    summaries = [
        {
            "id": board.get("id"),
            "workspace_id": board.get("workspace_id"),
            "workspace_app_id": board.get("workspace_app_id"),
            "name": board.get("name"),
            "description": board.get("description"),
            "key": board.get("key"),
            "icon": board.get("icon"),
            "color": board.get("color"),
            "position": board.get("position"),
            "next_issue_number": board.get("next_issue_number"),
            "created_at": board.get("created_at"),
            "updated_at": board.get("updated_at"),
            "url_path": f"/workspace/{board.get('workspace_id')}/projects/{board.get('id')}",
        }
        for board in boards
    ]

    return success(
        {"boards": summaries, "count": len(summaries)},
        f"Found {len(summaries)} project boards",
    )


@tool(
    name="get_project_board",
    description=(
        "Read a project board by ID, including its columns and issue summaries. Use this when the user "
        "mentions a board, asks about cards on a board, or references a board with a provided board ID."
    ),
    params={
        "board_id": "Project board ID to read",
        "state_id": "Optional: limit results to a single state/column ID",
        "assignee_user_id": "Optional: limit results to cards assigned to this user ID",
        "include_done": "Include cards already in done/completed states (default false)",
        "limit": "Maximum number of issue summaries to return (default 100, max 200)",
    },
    required=["board_id"],
    category=ToolCategory.PROJECTS,
    status="Reading project board..."
)
async def get_project_board(args: Dict, ctx: ToolContext) -> ToolResult:
    from api.services.projects import get_board_by_id, get_issues, get_labels, get_states

    board_id = args.get("board_id")
    if not board_id:
        return error("board_id is required")

    state_id = args.get("state_id")
    assignee_user_id = args.get("assignee_user_id")
    include_done = _coerce_bool(args.get("include_done"), default=False)
    limit = _clamp_limit(args.get("limit"), default=100, maximum=_MAX_BOARD_ISSUES)

    logger.info(
        "[CHAT] User %s reading project board %s (state=%s, assignee=%s, include_done=%s, limit=%s)",
        ctx.user_id,
        board_id,
        state_id,
        assignee_user_id,
        include_done,
        limit,
    )

    board = await get_board_by_id(ctx.user_jwt, board_id)
    if not board:
        return error("Project board not found")

    if ctx.workspace_ids and board.get("workspace_id") not in ctx.workspace_ids:
        return error("Project board is outside the current workspace scope")

    # Always fetch all issues (unfiltered) so state counts reflect the real board.
    # Then apply filters only to the issue list returned to the model.
    gather_args = [
        get_states(ctx.user_jwt, board_id),
        get_issues(ctx.user_jwt, board_id, include_done=True),
        get_labels(ctx.user_jwt, board_id),
        _get_member_map(board["workspace_id"], ctx.user_jwt),
    ]
    states, all_issues, labels, member_map = await asyncio.gather(*gather_args)

    state_lookup = {state["id"]: state for state in states}

    # --- Counts from ALL issues (unfiltered) ---
    issues_by_state: Dict[str, int] = {}
    open_by_state: Dict[str, int] = {}
    open_issue_count = 0
    for issue in all_issues:
        issue_state_id = issue.get("state_id")
        if issue_state_id:
            issues_by_state[issue_state_id] = issues_by_state.get(issue_state_id, 0) + 1
        is_done = bool(state_lookup.get(issue_state_id, {}).get("is_done")) or bool(issue.get("completed_at"))
        if not is_done:
            open_issue_count += 1
            if issue_state_id:
                open_by_state[issue_state_id] = open_by_state.get(issue_state_id, 0) + 1

    # --- Apply filters for the returned issue list ---
    filtered_issues = all_issues
    if not include_done:
        filtered_issues = [
            i for i in filtered_issues
            if not (bool(state_lookup.get(i.get("state_id"), {}).get("is_done")) or bool(i.get("completed_at")))
        ]
    if state_id:
        filtered_issues = [i for i in filtered_issues if i.get("state_id") == state_id]
    if assignee_user_id:
        filtered_issues = [
            i for i in filtered_issues
            if any(a.get("user_id") == assignee_user_id for a in (i.get("assignees") or []))
        ]

    sorted_issues = sorted(
        filtered_issues,
        key=lambda issue: (
            state_lookup.get(issue.get("state_id"), {}).get("position", 10**6),
            issue.get("position", 0),
            issue.get("number", 0),
        ),
    )

    issue_summaries = [
        _format_issue_summary(issue, state_lookup, member_map, board.get("key"))
        for issue in sorted_issues[:limit]
    ]

    state_summaries = [
        {
            "id": state.get("id"),
            "name": state.get("name"),
            "color": state.get("color"),
            "position": state.get("position"),
            "is_done": state.get("is_done", False),
            "issue_count": issues_by_state.get(state.get("id"), 0),
            "open_issue_count": open_by_state.get(state.get("id"), 0),
        }
        for state in states
    ]

    data = {
        "board": _format_board_summary(board, len(all_issues), open_issue_count),
        "states": state_summaries,
        "labels": _simplify_labels(labels),
        "issues": issue_summaries,
        "total_issue_count": len(all_issues),
        "filtered_issue_count": len(sorted_issues),
        "issues_returned": len(issue_summaries),
        "issues_truncated": len(sorted_issues) > len(issue_summaries),
        "filters": {
            "state_id": state_id,
            "assignee_user_id": assignee_user_id,
            "include_done": include_done,
        },
    }

    return success(
        data,
        f"Read board '{board.get('name', board_id)}' with {len(all_issues)} issues ({open_issue_count} open)",
    )


@tool(
    name="get_project_issue",
    description=(
        "Read a single project issue/card by ID with full description, labels, assignees, and state details. "
        "Use this for follow-up questions about one specific card."
    ),
    params={
        "issue_id": "Project issue/card ID to read",
    },
    required=["issue_id"],
    category=ToolCategory.PROJECTS,
    status="Reading project issue..."
)
async def get_project_issue(args: Dict, ctx: ToolContext) -> ToolResult:
    from api.services.projects import get_board_by_id, get_issue_by_id, get_states

    issue_id = args.get("issue_id")
    if not issue_id:
        return error("issue_id is required")

    logger.info("[CHAT] User %s reading project issue %s", ctx.user_id, issue_id)

    issue = await get_issue_by_id(ctx.user_jwt, issue_id)
    if not issue:
        return error("Project issue not found")

    if ctx.workspace_ids and issue.get("workspace_id") not in ctx.workspace_ids:
        return error("Project issue is outside the current workspace scope")

    board, states, member_map = await asyncio.gather(
        get_board_by_id(ctx.user_jwt, issue["board_id"]),
        get_states(ctx.user_jwt, issue["board_id"]),
        _get_member_map(issue["workspace_id"], ctx.user_jwt),
    )

    state_lookup = {state["id"]: state for state in states}
    state = state_lookup.get(issue.get("state_id"), {})
    board_key = board.get("key") if board else None

    issue_data: Dict[str, Any] = {
        "id": issue.get("id"),
        "board_id": issue.get("board_id"),
        "board_name": board.get("name") if board else None,
        "board_key": board_key,
        "workspace_id": issue.get("workspace_id"),
        "state_id": issue.get("state_id"),
        "state_name": state.get("name"),
        "is_done": bool(state.get("is_done")) or bool(issue.get("completed_at")),
        "number": issue.get("number"),
        "reference": _format_reference(board_key, issue.get("number")),
        "title": issue.get("title"),
        "description": issue.get("description"),
        "priority": issue.get("priority", 0),
        "due_at": issue.get("due_at"),
        "completed_at": issue.get("completed_at"),
        "labels": _simplify_labels(issue.get("label_objects")),
        "assignees": _simplify_assignees(issue.get("assignees"), member_map),
        "image_urls": issue.get("image_urls") or [],
        "image_count": len(issue.get("image_urls") or issue.get("image_r2_keys") or []),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "url_path": f"/workspace/{issue.get('workspace_id')}/projects?issue={issue.get('id')}",
    }

    return success(
        {"issue": issue_data},
        f"Read issue {issue_data.get('reference') or issue_id}",
    )
