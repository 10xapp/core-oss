"""
Tool definitions for the chat agent.

Each module in this package defines tools for a specific category
and registers them with the ToolRegistry on import.
"""

# Import all definition modules to trigger registration
from lib.tools.definitions import calendar
from lib.tools.definitions import email
from lib.tools.definitions import documents
from lib.tools.definitions import files
from lib.tools.definitions import web_search
from lib.tools.definitions import smart_search
from lib.tools.definitions import messages
from lib.tools.definitions import email_thread
from lib.tools.definitions import semantic_search
from lib.tools.definitions import projects
from lib.tools.definitions import memory

__all__ = [
    "calendar",
    "email",
    "documents",
    "files",
    "web_search",
    "smart_search",
    "messages",
    "email_thread",
    "semantic_search",
    "projects",
    "memory",
]
