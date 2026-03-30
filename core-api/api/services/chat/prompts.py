"""
Centralized prompt building for chat agent.
Handles system prompts, user context, and behavior instructions.
"""
import datetime
import logging
from typing import Any, Dict, List, Optional

from lib.supabase_client import get_authenticated_async_client
from api.config import settings
from lib import ultramemory_client as memory

logger = logging.getLogger(__name__)


# ============================================================================
# Date/Time Context
# ============================================================================

def get_datetime_context(user_timezone: str = "UTC") -> Dict[str, str]:
    """Get current date/time context for prompts in user's timezone."""
    from zoneinfo import ZoneInfo

    try:
        tz = ZoneInfo(user_timezone)
    except Exception:
        tz = ZoneInfo("UTC")

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_local = now_utc.astimezone(tz)
    tomorrow_local = now_local + datetime.timedelta(days=1)

    return {
        "iso": now_local.isoformat(),
        "date": now_local.strftime("%Y-%m-%d"),
        "date_readable": now_local.strftime("%B %d, %Y"),
        "day_of_week": now_local.strftime("%A"),
        "time": now_local.strftime("%H:%M"),
        "time_24h": now_local.strftime("%H:%M"),
        "timezone": user_timezone,
        "utc_offset": now_local.strftime("%z"),
        "tomorrow": tomorrow_local.strftime("%Y-%m-%d"),
        "tomorrow_readable": tomorrow_local.strftime("%B %d, %Y"),
    }


# ============================================================================
# User Preferences
# ============================================================================

async def get_user_preferences(user_id: str, user_jwt: str) -> Dict[str, Any]:
    """
    Fetch user preferences from database.
    Returns defaults if not found or on error.
    """
    defaults = {
        "show_embedded_cards": True,
        "always_search_content": True
    }

    try:
        supabase = await get_authenticated_async_client(user_jwt)
        result = await supabase.table("user_preferences").select("*").eq("user_id", user_id).execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        return defaults
    except Exception as e:
        logger.warning(f"Failed to fetch preferences for user {user_id}: {e}")
        return defaults


# ============================================================================
# Behavior Instructions
# ============================================================================

def build_behavior_instructions(preferences: Dict[str, Any]) -> str:
    """
    Build behavior instructions based on user preferences.
    This is where agent logic lives - add new preference-based behaviors here.
    """
    instructions = []

    # Embedded cards behavior
    show_embedded_cards = preferences.get("show_embedded_cards", True)

    if show_embedded_cards:
        instructions.append("""
=== RESPONSE FORMAT (CRITICAL) ===
The user has EMBEDDED CARDS ENABLED. The app displays interactive cards with full details.

IMPORTANT: DO NOT list items in your text response. The cards already show them.

GOOD responses:
- "Here are your 3 upcoming events." (cards show the details)
- "Found 5 emails matching your search." (cards show the emails)

BAD responses (NEVER DO THIS):
- "Here are your events: 1. Meeting at 9am with John... 2. Lunch at 12pm..."
- "I found these emails: From: john@... Subject: ..."

RULES:
1. State the count and a brief summary ONLY
2. NEVER list individual items - the cards handle that
3. Only give details if user asks about a SPECIFIC item

=== INLINE REFERENCES (EMAILS & CALENDAR) ===
Use {E1}, {E2} for emails and {C1}, {C2} for calendar events to create linked cards.
The index matches the order items appear in the tool result (1-based).

PLACEMENT: Place ALL reference tags together on their OWN LINE at the END of the relevant paragraph.
Do NOT put them mid-sentence — let the text flow naturally, then attach the references below.

GOOD example:
"You have a few actionable emails — PayPal needs you to confirm your email, and there's a security alert from Google worth checking.
{E1} {E3}

Your schedule today has 3 events: a sync at 10am, gym at 11:30, and an Apple appointment at 2:40pm.
{C1} {C2} {C3}"

BAD example (do NOT do this):
"PayPal {E1} needs you to confirm and Google {E3} sent an alert."

Rules:
- Place tags on their own line AFTER the paragraph that discusses them
- Group multiple tags together: {E1} {E2} {E3} or {C1} {C2}
- The number = the item's position in the tool results (1st email = {E1}, 2nd = {E2})
- Do NOT use tags when talking generally ("you have 5 emails") — only for specific items
- You can mix types if a paragraph covers both: {E1} {C2}""")
    else:
        instructions.append("""
=== RESPONSE FORMAT ===
The user has EMBEDDED CARDS DISABLED.
When you fetch lists (emails, calendar events):
- Provide full details in your text response
- Format clearly with bullet points or numbered lists
- Include all relevant information (subject, sender, time, etc.)""")

    # Smart search preference
    always_search_content = preferences.get("always_search_content", True)
    if always_search_content:
        instructions.append("""
=== SEARCH BEHAVIOR ===
Use "smart_search" for ALL search queries. DO NOT use search_emails.

Follow-ups ("latest one", "show me that", "the first one"):
- You MUST call smart_search again with the FULL contextualized query — never pass vague terms like "latest one".
- The user needs to SEE the embedded card, which only appears when the tool is called.
- Example: after "emails from Nike" → follow-up "latest one" → smart_search(query="from:nike", types="emails", limit="1")""")


    return "\n".join(instructions)


# ============================================================================
# Context Building
# ============================================================================

def build_context_string(context: Optional[Dict[str, Any]]) -> str:
    """Build a context string from provided emails and documents."""
    if not context:
        return ""

    context_parts = []

    # Add email context
    if context.get("emails"):
        context_parts.append("\n## Emails provided as context:")
        for i, email in enumerate(context["emails"], 1):
            email_str = f"\n### Email {i}:"
            if email.get("subject"):
                email_str += f"\n- Subject: {email['subject']}"
            if email.get("sender"):
                email_str += f"\n- From: {email['sender']}"
            if email.get("to"):
                email_str += f"\n- To: {email['to']}"
            if email.get("received_at"):
                email_str += f"\n- Date: {email['received_at']}"
            if email.get("body"):
                email_str += f"\n- Body:\n{email['body']}"
            elif email.get("snippet"):
                email_str += f"\n- Snippet: {email['snippet']}"
            context_parts.append(email_str)

    # Add document context
    if context.get("documents"):
        context_parts.append("\n## Documents provided as context:")
        for i, doc in enumerate(context["documents"], 1):
            doc_str = f"\n### Document {i}:"
            if doc.get("title"):
                doc_str += f"\n- Title: {doc['title']}"
            if doc.get("content"):
                doc_str += f"\n- Content:\n{doc['content']}"
            context_parts.append(doc_str)

    return "\n".join(context_parts)


# ============================================================================
# Main System Prompt Builder
# ============================================================================

async def build_system_prompt(
    user_id: str,
    user_jwt: str,
    context: Optional[Dict[str, Any]] = None,
    user_timezone: str = "UTC",
    workspace_ids: Optional[List[str]] = None,
) -> str:
    """
    Build complete system prompt with all context and preferences.

    Args:
        user_id: User ID
        user_jwt: User's JWT token for authenticated DB access
        context: Optional context dict with 'emails' and/or 'documents' lists
        user_timezone: User's timezone identifier (e.g., "Europe/Oslo")
        workspace_ids: Optional list of workspace IDs to scope tool results

    Returns:
        Complete system prompt string
    """
    # Get current datetime context in user's timezone
    dt_ctx = get_datetime_context(user_timezone)

    # Fetch user preferences and build behavior instructions
    preferences = await get_user_preferences(user_id, user_jwt)

    # Look up workspace names if scoped
    workspace_names: List[str] = []
    if workspace_ids:
        try:
            supabase = await get_authenticated_async_client(user_jwt)
            ws_result = await supabase.table("workspaces").select("name").in_("id", workspace_ids).execute()
            workspace_names = [ws.get("name") for ws in (ws_result.data or []) if ws.get("name")]
        except Exception as e:
            logger.warning(f"Failed to fetch workspace names for {workspace_ids}: {e}")
    behavior_instructions = build_behavior_instructions(preferences)

    # Build the base system prompt (consolidated — avoid repeating the same rules)
    base_prompt = f"""You are a helpful AI assistant for a productivity app with access to the user's calendar, emails, documents, files, project boards, and workspace messages.

=== DATE & TIME ===
Timezone: {dt_ctx['timezone']} (UTC{dt_ctx['utc_offset']})
Now: {dt_ctx['day_of_week']}, {dt_ctx['date_readable']} {dt_ctx['time']}
Tomorrow: {dt_ctx['tomorrow']}

ALL user times are in {dt_ctx['timezone']}. Never convert to UTC.
When no date is specified, use TODAY ({dt_ctx['date']}).
Time format: YYYY-MM-DDTHH:MM:SS (no Z suffix).
Example: "3pm" → {dt_ctx['date']}T15:00:00

=== TOOLS ===
Use "smart_search" for all search queries (emails, calendar, documents). It uses Gmail/Outlook native search when connected.
- Use Gmail operators for precision: from:name, subject:keyword
- For email threads: use get_email_thread with thread_id from search results

Channel messages:
- Use search_messages to find relevant messages by keyword
- Use get_channel_history to read a batch of messages in context:
  - Pass "around_message_id" from a search hit to read surrounding conversation
  - Omit to get the most recent messages; pass "before" timestamp to page back
- Always prefer reading context around a search hit over returning isolated matches

Project boards:
- Use list_project_boards to discover boards in the current workspace scope
- Use get_project_board to read a board's states and issue summaries
- Use get_project_issue to inspect one specific card in full detail
- If the user message includes markers like [User referenced: Name (project_board, id: ...)] or
  [User referenced: Name (project_issue, id: ...)], use the provided ID directly in the project tool call
- When the user asks about a mentioned board's cards, blockers, owners, or progress, call get_project_board first instead of guessing from the board name alone

Calendar operations:
- View: smart_search(types="calendar") or get_calendar_events with today_only/start_date/end_date
- Create: create_calendar_event with times in YYYY-MM-DDTHH:MM:SS format
- Update/Delete: FIRST smart_search to get event_id, THEN update_calendar_event or delete_calendar_event

When you need multiple tools, call them ALL at once — they execute in parallel.

=== STAGED ACTIONS (CRITICAL) ===
Some tools return status "staged" — this means the action is NOT done yet. It creates a confirmation card the user must tap to execute.
Staged tools: create_calendar_event, update_calendar_event, delete_calendar_event, send_email, create_todo, create_document, update_memory.
When a tool result says "staged", NEVER say the action is done/completed/added/created/sent.
Instead say something like "Here's the event ready to add" or "Tap to confirm" — keep it natural and brief.
{behavior_instructions}"""

    # Add workspace scope context
    if workspace_names:
        if len(workspace_names) == 1:
            base_prompt += f"""

=== WORKSPACE CONTEXT ===
You are operating within the "{workspace_names[0]}" workspace. When searching or listing data
(documents, files, project boards), scope your results to this workspace context.
When creating items (documents), associate them with this workspace."""
        else:
            names_list = ", ".join(f'"{n}"' for n in workspace_names)
            base_prompt += f"""

=== WORKSPACE CONTEXT ===
You are operating across these workspaces: {names_list}. When searching or listing data
(documents, files, project boards), scope your results to these workspaces.
When creating items (documents), associate them with the most relevant workspace."""

    # Add user-provided context if any
    context_str = build_context_string(context)
    if context_str:
        base_prompt += f"\n\nThe user has provided the following context for this conversation:{context_str}\n\nUse this context to help answer the user's questions. Reference specific details from the provided emails or documents when relevant."

    # ── Inject Ultramemory long-term context ──────────────────
    if settings.ultramemory_url:
        try:
            memory_context = await memory.startup_context(user_id)
            if memory_context:
                base_prompt += (
                    "\n\n=== LONG-TERM MEMORY ===\n"
                    "The following is from your long-term memory about this user. "
                    "Use it to personalize your responses, but don't mention that you're "
                    "reading from memory unless asked.\n\n"
                    f"{memory_context}"
                )
        except Exception as e:
            logger.warning(f"Memory context load failed (non-fatal): {e}")

    return base_prompt


# ============================================================================
# ADDING NEW AGENT LOGIC
# ============================================================================
#
# This module centralizes all AI behavior configuration. To add new agent logic:
#
# 1. ADD A NEW PREFERENCE (if needed):
#    - Add column to user_preferences table via Supabase migration
#    - Update iOS UserPreferences.swift and SettingsView.swift
#    - Update api/routers/preferences.py models
#
# 2. ADD BEHAVIOR INSTRUCTIONS:
#    In build_behavior_instructions(), add a new section:
#
#    ```python
#    # Example: verbosity preference
#    verbosity = preferences.get("response_verbosity", "normal")
#    if verbosity == "concise":
#        instructions.append("""
#    === RESPONSE LENGTH ===
#    Keep responses brief and to the point. Use bullet points.
#    Avoid lengthy explanations unless explicitly asked.""")
#    elif verbosity == "detailed":
#        instructions.append("""
#    === RESPONSE LENGTH ===
#    Provide thorough, detailed responses with context and examples.""")
#    ```
#
# 3. COMMON AGENT BEHAVIORS TO ADD:
#    - response_verbosity: "concise" | "normal" | "detailed"
#    - communication_style: "formal" | "casual" | "friendly"
#    - proactive_suggestions: bool (offer tips without being asked)
#    - auto_create_todos: bool (create todos from detected action items)
#    - calendar_defaults: default duration, reminder preferences
#    - email_style: signature, tone preferences
#
# 4. CONTEXT-AWARE BEHAVIORS:
#    For behaviors that depend on conversation context (not just preferences),
#    add parameters to build_system_prompt() and handle in the base prompt.
#
#    Example: time-of-day greetings, weekend vs weekday behavior, etc.
#
# 5. TESTING:
#    - Toggle preference in Settings
#    - Ask relevant questions in chat
#    - Verify AI response matches expected behavior
#
# ============================================================================
