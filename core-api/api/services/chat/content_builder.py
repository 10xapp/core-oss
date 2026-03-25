"""
Content builder for converting message content to structured content parts.

This module handles parsing text with [N] citations into structured content parts
that can be rendered directly by iOS without regex parsing.

Content Part Schema:
{
    "id": "uuid",
    "type": "text" | "source_ref" | "tool_result" | "action" | "reasoning" | "sources",
    "phase": "grounded" | "result" | "reasoning" | "footer",  # Optional metadata
    "data": { ... type-specific data ... }
}

Content Part Types:
- text: {"content": "markdown text"}
- source_ref: {"source_index": 1}  # 1-based index
- email_ref: {"email_index": 1}  # 1-based index into email display items
- cal_ref: {"cal_index": 1}  # 1-based index into calendar display items
- tool_result: {"display_type": "calendar_events", "items": [...], "total_count": 5}
- action: {"action": "create_calendar_event", "status": "staged", "data": {...}, "description": "..."}
- reasoning: {"content": "synthesis text"}  # Post-tool analysis
- sources: {"sources": [{"url": "...", "title": "...", "domain": "...", "favicon": "..."}]}

Note: "display" is deprecated in favor of "tool_result" but both are supported for backward compatibility.
"""

import re
import uuid
from typing import List, Dict, Any, Optional

# Pattern to match citation markers like [1], [2], etc.
CITATION_PATTERN = re.compile(r'\[(\d+)\]')

# Pattern to match email reference markers like {E1}, {E2}, etc.
EMAIL_REF_PATTERN = re.compile(r'\{E(\d+)\}')

# Pattern to match calendar reference markers like {C1}, {C2}, etc.
CAL_REF_PATTERN = re.compile(r'\{C(\d+)\}')

# Combined pattern for all inline references (citations, email refs, calendar refs)
ALL_REF_PATTERN = re.compile(r'(\[(\d+)\]|\{E(\d+)\}|\{C(\d+)\})')

# Pattern to move citations before punctuation to after punctuation
# Matches: [1]. or [1], or [1]! or [1]? or [1][2]. etc.
CITATION_BEFORE_PUNCT_PATTERN = re.compile(r'((?:\[\d+\])+)([.!?,])')

# Pattern to move email/calendar refs before punctuation to after punctuation
EMAIL_REF_BEFORE_PUNCT_PATTERN = re.compile(r'((?:\{E\d+\})+)([.!?,])')
CAL_REF_BEFORE_PUNCT_PATTERN = re.compile(r'((?:\{C\d+\})+)([.!?,])')


def fix_citation_placement(text: str) -> str:
    """
    Move citations/email refs that appear before punctuation to after punctuation.

    Example:
        "This is a fact [1]." -> "This is a fact. [1]"
        "Multiple sources [1][2]." -> "Multiple sources. [1][2]"
        "PayPal email {E1}." -> "PayPal email. {E1}"
    """
    text = CITATION_BEFORE_PUNCT_PATTERN.sub(r'\2 \1', text)
    text = EMAIL_REF_BEFORE_PUNCT_PATTERN.sub(r'\2 \1', text)
    text = CAL_REF_BEFORE_PUNCT_PATTERN.sub(r'\2 \1', text)
    return text


def generate_part_id() -> str:
    """Generate a unique content part ID."""
    return str(uuid.uuid4())


def parse_text_to_parts(text: str, phase: str = "grounded") -> List[Dict[str, Any]]:
    """
    Parse text with [N] citations and {EN} email refs into structured content parts.

    Args:
        text: The raw text content with [N] citation markers and/or {EN} email ref markers
        phase: The content phase ("grounded" or "reasoning")

    Returns:
        List of content part dicts with id, type, phase, and data fields

    Example:
        Input: "According to research [1], check the PayPal email. {E1}"
        Output: [
            {"id": "...", "type": "text", "phase": "grounded", "data": {"content": "According to research "}},
            {"id": "...", "type": "source_ref", "phase": "grounded", "data": {"source_index": 1}},
            {"id": "...", "type": "text", "phase": "grounded", "data": {"content": ", check the PayPal email. "}},
            {"id": "...", "type": "email_ref", "phase": "grounded", "data": {"email_index": 1}},
        ]
    """
    if not text:
        return []

    # Fix citation placement: move citations/email refs before punctuation to after
    text = fix_citation_placement(text)

    parts = []
    last_end = 0

    for match in ALL_REF_PATTERN.finditer(text):
        # Add text before the reference (if any)
        if match.start() > last_end:
            text_before = text[last_end:match.start()]
            if text_before:
                parts.append({
                    "id": generate_part_id(),
                    "type": "text",
                    "phase": phase,
                    "data": {"content": text_before}
                })

        if match.group(2) is not None:
            # Source citation: [N]
            source_index = int(match.group(2))
            parts.append({
                "id": generate_part_id(),
                "type": "source_ref",
                "phase": phase,
                "data": {"source_index": source_index}
            })
        elif match.group(3) is not None:
            # Email reference: {EN}
            email_index = int(match.group(3))
            parts.append({
                "id": generate_part_id(),
                "type": "email_ref",
                "phase": phase,
                "data": {"email_index": email_index}
            })
        elif match.group(4) is not None:
            # Calendar reference: {CN}
            cal_index = int(match.group(4))
            parts.append({
                "id": generate_part_id(),
                "type": "cal_ref",
                "phase": phase,
                "data": {"cal_index": cal_index}
            })

        last_end = match.end()

    # Add remaining text after last reference
    if last_end < len(text):
        remaining = text[last_end:]
        if remaining:
            parts.append({
                "id": generate_part_id(),
                "type": "text",
                "phase": phase,
                "data": {"content": remaining}
            })

    # If no references found, return single text part
    if not parts and text:
        parts.append({
            "id": generate_part_id(),
            "type": "text",
            "phase": phase,
            "data": {"content": text}
        })

    return parts


def merge_adjacent_text_parts(parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge consecutive text parts into one for cleaner output.

    This is useful after modifications that might leave adjacent text parts.
    """
    if not parts:
        return parts

    merged = []
    for part in parts:
        if (part["type"] == "text" and
            merged and
            merged[-1]["type"] == "text"):
            # Merge with previous text part
            merged[-1]["data"]["content"] += part["data"]["content"]
        else:
            merged.append(part)

    return merged


def create_action_part(action: str, data: Dict[str, Any], description: str, action_id: str = None) -> Dict[str, Any]:
    """Create an action content part for staged operations.

    Args:
        action: Action type (e.g., "create_todo", "send_email")
        data: Action-specific data
        description: Human-readable description
        action_id: Optional pre-generated ID (for streaming consistency). If None, generates new ID.
    """
    return {
        "id": action_id or generate_part_id(),
        "type": "action",
        "phase": "result",
        "data": {
            "action": action,
            "status": "staged",
            "data": data,
            "description": description
        }
    }


def create_tool_result_part(display_type: str, items: List[Dict], total_count: int) -> Dict[str, Any]:
    """Create a tool_result content part for embedded cards (calendar, email, todos)."""
    return {
        "id": generate_part_id(),
        "type": "tool_result",
        "phase": "result",
        "data": {
            "display_type": display_type,
            "items": items,
            "total_count": total_count
        }
    }


def create_tool_call_part(tool_use_id: str, name: str, args: Dict[str, Any], result_json: str) -> Dict[str, Any]:
    """Create a tool_call content part capturing the full tool exchange for history reconstruction."""
    return {
        "id": generate_part_id(),
        "type": "tool_call",
        "phase": "result",
        "data": {
            "tool_use_id": tool_use_id,
            "name": name,
            "args": args,
            "result": result_json,
        }
    }


# Backward compatibility alias
def create_display_part(display_type: str, items: List[Dict], total_count: int) -> Dict[str, Any]:
    """Create a display content part (deprecated, use create_tool_result_part)."""
    return create_tool_result_part(display_type, items, total_count)


def create_reasoning_part(content: str) -> Dict[str, Any]:
    """Create a reasoning content part for post-tool synthesis."""
    return {
        "id": generate_part_id(),
        "type": "reasoning",
        "phase": "reasoning",
        "data": {
            "content": content
        }
    }


def create_sources_part(sources: List[Dict[str, str]]) -> Dict[str, Any]:
    """Create a sources content part for citation footer."""
    return {
        "id": generate_part_id(),
        "type": "sources",
        "phase": "footer",
        "data": {
            "sources": sources
        }
    }


def create_attachment_part(
    attachment_id: str,
    filename: str,
    mime_type: str,
    file_size: int,
    r2_key: str,
    thumbnail_r2_key: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> Dict[str, Any]:
    """Create an attachment content part for images/files in chat messages.

    Args:
        attachment_id: UUID of the chat_attachment record
        filename: Original filename
        mime_type: MIME type (e.g., image/jpeg)
        file_size: File size in bytes
        r2_key: R2 storage key for original file
        thumbnail_r2_key: R2 storage key for thumbnail (optional)
        width: Image width in pixels (optional)
        height: Image height in pixels (optional)

    Returns:
        Content part dict for attachment
    """
    return {
        "id": generate_part_id(),
        "type": "attachment",
        "data": {
            "attachment_id": attachment_id,
            "filename": filename,
            "mime_type": mime_type,
            "file_size": file_size,
            "r2_key": r2_key,
            "thumbnail_r2_key": thumbnail_r2_key,
            "width": width,
            "height": height,
        }
    }


class ContentBuilder:
    """
    Stateful content builder that maintains interleaving order during streaming.

    Supports phase tracking for distinguishing grounded text from reasoning.

    Usage:
        builder = ContentBuilder()
        builder.append_text("Hello ")
        builder.append_text("world")
        builder.flush_text()  # Creates text part(s) with citations parsed
        builder.add_tool_result(...)  # Adds tool result part
        builder.set_reasoning_phase()  # Switch to reasoning mode
        builder.append_text("Based on this...")  # This will become reasoning
        parts = builder.finalize(sources)  # Flushes remaining text, adds sources
    """

    def __init__(self) -> None:
        self.content_parts: List[Dict[str, Any]] = []
        self.current_text: str = ""
        self.reasoning_text: str = ""
        self.is_reasoning_phase: bool = False

    def append_text(self, text: str) -> None:
        """Accumulate text content."""
        if self.is_reasoning_phase:
            self.reasoning_text += text
        else:
            self.current_text += text

    def flush_text(self) -> None:
        """Flush accumulated grounded text to content parts (parses citations)."""
        if self.current_text:
            text_parts = parse_text_to_parts(self.current_text, phase="grounded")
            self.content_parts.extend(text_parts)
            self.current_text = ""

    def set_reasoning_phase(self) -> None:
        """
        Switch to reasoning phase.

        Text appended after this call will be accumulated as reasoning,
        not grounded text.
        """
        self.flush_text()  # Flush any pending grounded text first
        self.is_reasoning_phase = True

    def add_tool_result(self, display_type: str, items: List[Dict], total_count: int) -> None:
        """Add a tool_result content part (flushes text first to preserve order)."""
        self.flush_text()
        self.content_parts.append(create_tool_result_part(display_type, items, total_count))

    # Backward compatibility alias
    def add_display(self, display_type: str, items: List[Dict], total_count: int) -> None:
        """Add a display content part (deprecated, use add_tool_result)."""
        self.add_tool_result(display_type, items, total_count)

    def add_tool_call(self, tool_use_id: str, name: str, args: Dict[str, Any], result_json: str) -> None:
        """Add a tool_call content part capturing the full tool exchange (flushes text first)."""
        self.flush_text()
        self.content_parts.append(create_tool_call_part(tool_use_id, name, args, result_json))

    def add_action(self, action: str, data: Dict[str, Any], description: str, action_id: str = None) -> None:
        """Add an action content part (flushes text first to preserve order).

        Args:
            action: Action type (e.g., "create_todo", "send_email")
            data: Action-specific data
            description: Human-readable description
            action_id: Optional pre-generated ID (for streaming consistency)
        """
        self.flush_text()
        self.content_parts.append(create_action_part(action, data, description, action_id))

    def add_sources(self, sources: List[Dict[str, str]]) -> None:
        """Add sources content part inline (flushes text first to preserve order).

        Called when [SOURCES] marker is detected in stream, ensuring sources
        appear at the correct position (after web search content, before other content).
        """
        self.flush_text()
        if sources:
            self.content_parts.append(create_sources_part(sources))

    def add_attachment(
        self,
        attachment_id: str,
        filename: str,
        mime_type: str,
        file_size: int,
        r2_key: str,
        thumbnail_r2_key: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
    ) -> None:
        """Add an attachment content part (for user message with images).

        Note: This does NOT flush text first, as attachments typically come
        before the user's text in a message.
        """
        self.content_parts.append(create_attachment_part(
            attachment_id=attachment_id,
            filename=filename,
            mime_type=mime_type,
            file_size=file_size,
            r2_key=r2_key,
            thumbnail_r2_key=thumbnail_r2_key,
            width=width,
            height=height,
        ))

    def finalize(self, sources: List[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Finalize and return all content parts.

        Flushes remaining grounded text, adds reasoning block if present,
        and adds sources footer.

        Args:
            sources: Optional sources to add as footer (if not already added inline)

        Returns:
            Complete content_parts array ready for database storage
        """
        # Flush any remaining grounded text
        self.flush_text()

        # Add reasoning block if we have reasoning text
        if self.reasoning_text:
            self.content_parts.append(create_reasoning_part(self.reasoning_text))
            self.reasoning_text = ""

        # Add sources footer if provided and not already present
        if sources:
            # Check if sources already added (avoid duplicates)
            has_sources = any(p["type"] == "sources" for p in self.content_parts)
            if not has_sources:
                self.content_parts.append(create_sources_part(sources))

        return self.content_parts


def build_message_content_parts(
    text_content: str,
    sources: List[Dict[str, str]] = None,
    display_events: List[Dict[str, Any]] = None,
    action_events: List[Dict[str, Any]] = None,
    reasoning_content: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Build a complete content_parts array for a message (legacy interface).

    NOTE: This function does NOT preserve interleaving order because it receives
    all events after streaming completes. For proper interleaving, use ContentBuilder
    during streaming.

    Args:
        text_content: The raw text with [N] citations
        sources: List of source dicts for the sources footer
        display_events: List of display event dicts (calendar, email, todo cards)
        action_events: List of action event dicts (staged operations)
        reasoning_content: Optional reasoning/synthesis text

    Returns:
        Complete content_parts array ready for database storage
    """
    parts = []

    # 1. Parse text content into text/source_ref parts
    if text_content:
        text_parts = parse_text_to_parts(text_content, phase="grounded")
        parts.extend(text_parts)

    # 2. Add tool_result parts (these were interleaved during streaming)
    if display_events:
        for event in display_events:
            parts.append(create_tool_result_part(
                display_type=event.get("display_type", ""),
                items=event.get("items", []),
                total_count=event.get("total_count", 0)
            ))

    # 3. Add action parts
    if action_events:
        for event in action_events:
            parts.append(create_action_part(
                action=event.get("action", ""),
                data=event.get("data", {}),
                description=event.get("description", "")
            ))

    # 4. Add reasoning part if provided
    if reasoning_content:
        parts.append(create_reasoning_part(reasoning_content))

    # 5. Add sources part at the end (for citation footer)
    if sources:
        parts.append(create_sources_part(sources))

    return parts
