"""
Base classes and types for the tool registry system.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Literal
from enum import Enum


class ToolCategory(str, Enum):
    """Categories for grouping related tools."""
    CALENDAR = "calendar"
    EMAIL = "email"
    DOCUMENTS = "documents"
    FILES = "files"
    MEMORY = "memory"
    WEB = "web"
    MESSAGES = "messages"
    SEARCH = "search"
    PROJECTS = "projects"


@dataclass
class ToolDefinition:
    """
    Definition of a tool that can be called by the LLM.

    Attributes:
        name: Unique identifier for the tool
        description: Human-readable description for the LLM
        parameters: JSON Schema for tool parameters (MCP: inputSchema)
        required_params: List of required parameter names
        category: Category for grouping tools
        requires_connection: List of provider names required (e.g., ["google"])
        is_staged: Whether this tool requires user confirmation
        status_message: Message to show while tool is executing
        title: Human-readable display name (MCP compatibility)
        output_schema: JSON Schema for structured output (MCP compatibility)
        annotations: Behavior hints (MCP compatibility)
    """
    name: str
    description: str
    parameters: Dict[str, Any]
    required_params: List[str] = field(default_factory=list)
    category: ToolCategory = ToolCategory.WEB
    requires_connection: Optional[List[str]] = None
    is_staged: bool = False
    status_message: Optional[str] = None
    # MCP compatibility fields
    title: Optional[str] = None
    output_schema: Optional[Dict[str, Any]] = None
    annotations: Optional[Dict[str, Any]] = None


@dataclass
class ToolContext:
    """
    Context passed to tool handlers during execution.

    Attributes:
        user_id: The authenticated user's ID
        user_jwt: JWT token for API calls
        user_timezone: User's timezone (e.g., "Europe/Oslo")
        ext_connections: List of active connection providers for this user
    """
    user_id: str
    user_jwt: str
    user_timezone: str = "UTC"
    ext_connections: List[str] = field(default_factory=list)
    workspace_ids: Optional[List[str]] = None


@dataclass
class ToolResult:
    """
    Result returned from tool execution.

    Attributes:
        status: "success", "staged", or "error"
        data: Tool-specific result data
        display_type: Type of embedded display (e.g., "calendar_events")
        display_items: Items to display in embedded cards
        display_total: Total count of items (for "See all N" button)
        sources: Web search sources for citation display
        description: Human-readable description (for staged actions)
    """
    status: Literal["success", "staged", "error"]
    data: Dict[str, Any] = field(default_factory=dict)
    display_type: Optional[str] = None
    display_items: Optional[List[Dict]] = None
    display_total: Optional[int] = None
    sources: Optional[List[Dict[str, str]]] = None
    description: Optional[str] = None

    def to_json_string(self) -> str:
        """Convert result to JSON string for LLM context."""
        import json
        result = {"status": self.status}

        if self.status == "error":
            result["error"] = self.data.get("error", "Unknown error")
        elif self.status == "staged":
            result["action"] = self.data.get("action", "")
            result["data"] = self.data
            result["description"] = self.description or ""
            result["note"] = (
                "This action is STAGED, NOT executed. "
                "The user will see a confirmation card and must tap to confirm. "
                "Do NOT say the action is done. Say it's ready for them to confirm."
            )
        else:
            result["data"] = self.data
            if self.display_type:
                result["display_type"] = self.display_type
                result["display_items"] = self.display_items or []
                result["display_total"] = self.display_total or 0
            if self.sources:
                result["sources"] = self.sources

        return json.dumps(result)


# =============================================================================
# RESULT HELPERS - Shorthand functions for common result patterns
# =============================================================================

def staged_result(action: str, args: Dict[str, Any], description: str) -> ToolResult:
    """Create a staged action result requiring user confirmation."""
    return ToolResult(
        status="staged",
        data={"action": action, **args},
        description=description
    )


def display_result(
    data: Dict[str, Any],
    display_type: str,
    items: List[Dict],
    total: int = None,
    description: str = None
) -> ToolResult:
    """Create a success result with embedded display cards."""
    return ToolResult(
        status="success",
        data=data,
        display_type=display_type,
        display_items=items,
        display_total=total or len(items),
        description=description
    )


def success(data: Dict[str, Any], description: str = None) -> ToolResult:
    """Create a simple success result."""
    return ToolResult(status="success", data=data, description=description)


def error(message: str) -> ToolResult:
    """Create an error result."""
    return ToolResult(status="error", data={"error": message})
