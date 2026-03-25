"""
Central Tool Registry for managing tool definitions and execution.

Provides:
- ToolRegistry: Central class for registering and executing tools
- @tool decorator: Compact way to define and register tools
"""

from typing import Dict, List, Callable, Awaitable, Optional, Any
from functools import wraps
import logging

from lib.tools.base import ToolDefinition, ToolCategory, ToolContext, ToolResult

logger = logging.getLogger(__name__)

# Type alias for async tool handlers
ToolHandler = Callable[[Dict, ToolContext], Awaitable[ToolResult]]


# =============================================================================
# @tool DECORATOR - Compact tool registration
# =============================================================================

def tool(
    name: str,
    description: str,
    params: Dict[str, str] = None,
    required: List[str] = None,
    category: ToolCategory = ToolCategory.WEB,
    connection: str = None,
    staged: bool = False,
    status: str = None,
    title: str = None,
    output_schema: Dict[str, Any] = None
):
    """
    Decorator to register a tool with minimal boilerplate.

    Args:
        name: Unique tool identifier
        description: What the tool does (shown to LLM)
        params: Dict of param_name -> description (types auto-inferred)
        required: List of required parameter names
        category: Tool category (CALENDAR, EMAIL, TODOS, etc.)
        connection: Required OAuth connection (e.g., "google")
        staged: True if requires user confirmation
        status: Message shown during execution
        title: Human-readable display name (MCP)
        output_schema: JSON Schema for structured output (MCP)

    Type inference from param names:
        - *_only, is_*, include_* → boolean
        - max_results, limit, num_results, priority → integer
        - tags → array of strings
        - everything else → string

    Example:
        @tool(
            name="get_todos",
            description="Get user's todo list",
            params={"include_completed": "Include completed todos"},
            category=ToolCategory.TODOS,
            status="Checking your tasks..."
        )
        async def get_todos(args: Dict, ctx: ToolContext) -> ToolResult:
            ...
    """
    def decorator(handler: ToolHandler):
        # Build JSON Schema properties from simple dict
        properties = {}
        for param, desc in (params or {}).items():
            # Auto-infer type from param name
            if param.endswith("_only") or param.startswith("is_") or param.startswith("include_"):
                prop_type = "boolean"
            elif param in ("max_results", "limit", "num_results", "priority"):
                prop_type = "integer"
            elif param == "tags":
                properties[param] = {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": desc
                }
                continue
            else:
                prop_type = "string"
            properties[param] = {"type": prop_type, "description": desc}

        tool_def = ToolDefinition(
            name=name,
            description=description,
            parameters={"type": "object", "properties": properties},
            required_params=required or [],
            category=category,
            requires_connection=[connection] if connection else None,
            is_staged=staged,
            status_message=status,
            title=title,
            output_schema=output_schema
        )
        ToolRegistry.register(tool_def, handler)

        @wraps(handler)
        async def wrapper(*args, **kwargs):
            return await handler(*args, **kwargs)
        return wrapper

    return decorator


class ToolRegistry:
    """
    Central registry for tool definitions and handlers.

    Usage:
        # Register a tool
        ToolRegistry.register(my_tool_def, my_handler)

        # Get tools for a user (filtered by connections)
        tools = ToolRegistry.get_openai_tools(["google", "microsoft"])

        # Execute a tool
        result = await ToolRegistry.execute("web_search", {"query": "..."}, context)
    """

    _tools: Dict[str, ToolDefinition] = {}
    _handlers: Dict[str, ToolHandler] = {}

    @classmethod
    def register(cls, tool: ToolDefinition, handler: ToolHandler) -> None:
        """
        Register a tool definition and its handler.

        Args:
            tool: The tool definition
            handler: Async function that executes the tool
        """
        cls._tools[tool.name] = tool
        cls._handlers[tool.name] = handler
        logger.debug(f"Registered tool: {tool.name}")

    @classmethod
    def get_tool(cls, name: str) -> Optional[ToolDefinition]:
        """Get a tool definition by name."""
        return cls._tools.get(name)

    @classmethod
    def get_status_message(cls, name: str) -> Optional[str]:
        """Get the status message for a tool."""
        tool = cls._tools.get(name)
        return tool.status_message if tool else None

    @classmethod
    def get_tools_for_user(cls, ext_connections: List[str]) -> List[ToolDefinition]:
        """
        Filter tools based on user's connected services.

        Args:
            ext_connections: List of provider names the user has connected
                             (e.g., ["google", "microsoft"])

        Returns:
            List of tools available to this user
        """
        available = []
        for tool in cls._tools.values():
            if tool.requires_connection is None:
                # Tool doesn't require any connection
                available.append(tool)
            elif any(conn in ext_connections for conn in tool.requires_connection):
                # User has at least one required connection
                available.append(tool)
        return available

    @classmethod
    def get_openai_tools(cls, ext_connections: List[str]) -> List[Dict]:
        """
        Get tools in OpenAI function calling format.

        Args:
            ext_connections: List of provider names the user has connected

        Returns:
            List of tool definitions in OpenAI format
        """
        from lib.tools.adapters.openai import to_openai_format
        tools = cls.get_tools_for_user(ext_connections)
        return [to_openai_format(t) for t in tools]

    @classmethod
    def get_claude_tools(cls, ext_connections: List[str]) -> List[Dict]:
        """
        Get tools in Claude tool_use format.

        Args:
            ext_connections: List of provider names the user has connected

        Returns:
            List of tool definitions in Claude format
        """
        from lib.tools.adapters.claude import to_claude_format
        tools = cls.get_tools_for_user(ext_connections)
        return [to_claude_format(t) for t in tools]

    @classmethod
    def get_mcp_tools(cls, ext_connections: List[str]) -> List[Dict]:
        """
        Get tools in MCP (Model Context Protocol) format.

        Args:
            ext_connections: List of provider names the user has connected

        Returns:
            List of tool definitions in MCP tools/list format
        """
        from lib.tools.adapters.mcp import to_mcp_format
        tools = cls.get_tools_for_user(ext_connections)
        return [to_mcp_format(t) for t in tools]

    @classmethod
    async def execute(cls, name: str, args: Dict, context: ToolContext) -> ToolResult:
        """
        Execute a tool by name.

        Args:
            name: Tool name
            args: Tool arguments from LLM
            context: Execution context with user info

        Returns:
            ToolResult with status and data
        """
        handler = cls._handlers.get(name)
        if not handler:
            logger.error(f"Unknown tool: {name}")
            return ToolResult(
                status="error",
                data={"error": f"Unknown tool: {name}"}
            )

        try:
            return await handler(args, context)
        except Exception as e:
            logger.exception(f"Error executing tool {name}: {e}")
            return ToolResult(
                status="error",
                data={"error": "An error occurred while executing the tool"}
            )

    @classmethod
    def clear(cls) -> None:
        """Clear all registered tools (useful for testing)."""
        cls._tools.clear()
        cls._handlers.clear()

    @classmethod
    def list_tools(cls) -> List[str]:
        """List all registered tool names."""
        return list(cls._tools.keys())
