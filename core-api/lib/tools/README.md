# Tool Registry

Modular, LLM-agnostic tool system with automatic format conversion for OpenAI, Claude, and MCP.

## Quick Start: Adding a New Tool

```python
from lib.tools.base import ToolCategory, ToolContext, ToolResult, success, error, display_result, staged_result
from lib.tools.registry import tool

@tool(
    name="my_tool",
    description="What the tool does",
    params={
        "required_param": "Description of required param",
        "optional_param": "Description of optional param"
    },
    required=["required_param"],
    category=ToolCategory.CALENDAR,  # CALENDAR, EMAIL, TODOS, DOCUMENTS, FILES, MEMORY, WEB
    connection="google",              # Omit if no OAuth needed
    staged=False,                     # True = requires user confirmation
    status="Working..."               # Shown during execution (optional)
)
async def my_tool(args: Dict, ctx: ToolContext) -> ToolResult:
    # Your implementation
    result = await some_service(ctx.user_id, ctx.user_jwt)
    return success(result, "Operation completed")
```

## Result Helpers

| Helper | Use Case | Example |
|--------|----------|---------|
| `success(data, desc)` | Simple success | `success({"count": 5}, "Found 5 items")` |
| `error(message)` | Error response | `error("File not found")` |
| `display_result(data, type, items, total, desc)` | Success with UI cards | `display_result(result, "calendar_events", events, len(events))` |
| `staged_result(action, args, desc)` | Requires user confirmation | `staged_result("send_email", args, "Send to bob@...")` |

## Type Inference

Parameter types are auto-inferred from names:

| Pattern | Inferred Type |
|---------|---------------|
| `*_only`, `is_*`, `include_*` | boolean |
| `max_results`, `limit`, `num_results`, `priority` | integer |
| `tags` | array of strings |
| Everything else | string |

## Categories

```python
class ToolCategory(str, Enum):
    CALENDAR = "calendar"
    EMAIL = "email"
    TODOS = "todos"
    DOCUMENTS = "documents"
    FILES = "files"
    MEMORY = "memory"
    WEB = "web"
```

## Connection-Based Filtering

Tools that require OAuth connections specify them via `connection`:

```python
@tool(
    name="get_calendar_events",
    connection="google",  # Only available if user has Google connected
    ...
)
```

The registry automatically filters tools based on user's connected services:

```python
# Get tools for a user with Google connected
tools = ToolRegistry.get_openai_tools(["google"])

# Get tools for a user with no connections
tools = ToolRegistry.get_openai_tools([])  # Only non-connection tools
```

## LLM Format Adapters

```python
# OpenAI function calling format
tools = ToolRegistry.get_openai_tools(user_connections)

# Claude tool_use format
tools = ToolRegistry.get_claude_tools(user_connections)

# MCP (Model Context Protocol) format
tools = ToolRegistry.get_mcp_tools(user_connections)
```

## Executing Tools

```python
from lib.tools.base import ToolContext
from lib.tools.registry import ToolRegistry

context = ToolContext(
    user_id="user_123",
    user_jwt="jwt_token",
    user_timezone="America/New_York",
    ext_connections=["google"]
)

result = await ToolRegistry.execute("get_calendar_events", {"today_only": True}, context)
```

## MCP Compatibility

The registry is designed for future MCP server exposure:

```python
@tool(
    name="search_documents",
    title="Document Search",           # MCP: Human-readable display name
    output_schema={"type": "object"},  # MCP: Structured output validation
    ...
)
```

## File Structure

```
lib/tools/
├── __init__.py           # Exports
├── base.py               # ToolDefinition, ToolContext, ToolResult, helpers
├── registry.py           # ToolRegistry class, @tool decorator
├── definitions/          # Tool implementations
│   ├── calendar.py       # get_calendar_events, create_calendar_event
│   ├── email.py          # search_emails, send_email
│   ├── todos.py          # get_todos, create_todo, complete_todo
│   ├── documents.py      # list_documents, get_document, create_document
│   ├── files.py          # list_files, get_file_url
│   ├── memory.py         # update_memory
│   └── web_search.py     # web_search
└── adapters/             # Format converters
    ├── openai.py         # OpenAI function calling
    ├── claude.py         # Claude tool_use
    └── mcp.py            # Model Context Protocol
```

## Complete Example

```python
"""Example: Adding a weather tool"""

import logging
from typing import Dict

from lib.tools.base import ToolCategory, ToolContext, ToolResult, success, error
from lib.tools.registry import tool

logger = logging.getLogger(__name__)

@tool(
    name="get_weather",
    description="Get current weather for a location",
    params={
        "location": "City name or zip code",
        "units": "Temperature units: 'celsius' or 'fahrenheit'"
    },
    required=["location"],
    category=ToolCategory.WEB,
    status="Checking weather..."
)
async def get_weather(args: Dict, ctx: ToolContext) -> ToolResult:
    from api.services.weather import fetch_weather

    location = args.get("location")
    units = args.get("units", "fahrenheit")

    logger.info(f"[CHAT] User {ctx.user_id} fetching weather for {location}")

    try:
        weather = await fetch_weather(location, units)
        return success(weather, f"Weather for {location}: {weather['temp']}°")
    except Exception as e:
        logger.error(f"Weather fetch failed: {e}")
        return error(f"Could not get weather for {location}")
```

## Testing

```python
from lib.tools.registry import ToolRegistry

# List all registered tools
print(ToolRegistry.list_tools())

# Get tool definition
tool_def = ToolRegistry.get_tool("get_calendar_events")
print(tool_def.description)

# Clear registry (for tests)
ToolRegistry.clear()
```
