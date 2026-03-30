"""Shared typed request values for API request models."""

from enum import StrEnum


class WorkspaceRole(StrEnum):
    MEMBER = "member"
    ADMIN = "admin"


class WorkspaceAppType(StrEnum):
    CHAT = "chat"
    FILES = "files"
    MESSAGES = "messages"
    DASHBOARD = "dashboard"
    PROJECTS = "projects"
    EMAIL = "email"
    CALENDAR = "calendar"
    AGENTS = "agents"


class ResourceType(StrEnum):
    WORKSPACE_APP = "workspace_app"
    FOLDER = "folder"
    DOCUMENT = "document"
    FILE = "file"
    PROJECT_BOARD = "project_board"
    CHANNEL = "channel"


class PermissionLevel(StrEnum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class AccessRequestStatus(StrEnum):
    APPROVED = "approved"
    DENIED = "denied"
