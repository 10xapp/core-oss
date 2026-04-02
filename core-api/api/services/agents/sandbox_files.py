"""
Sandbox file browsing — list and read files from a running E2B sandbox.
"""
import asyncio
import logging
import posixpath
import re
import shlex
from typing import List, Dict, Any

from e2b import Sandbox

from api.config import settings
from lib.supabase_client import get_authenticated_async_client

logger = logging.getLogger(__name__)

# Allowed root for sandbox file operations
_SANDBOX_ROOT = "/home/user"


def _sanitize_sandbox_path(path: str) -> str:
    """Sanitize a file path for use in sandbox shell commands.

    - Resolves .. and . segments
    - Ensures the resolved path stays under _SANDBOX_ROOT
    - Shell-quotes the result to prevent argument injection
    """
    # Normalize and resolve relative segments (posixpath for Linux sandbox)
    resolved = posixpath.normpath(path)

    # Ensure it stays within the sandbox root
    if not resolved.startswith(_SANDBOX_ROOT + "/") and resolved != _SANDBOX_ROOT:
        raise ValueError(f"Path must be under {_SANDBOX_ROOT}")

    return shlex.quote(resolved)


def _get_e2b_api_key() -> str:
    key = settings.e2b_api_key
    if not key:
        raise ValueError("E2B_API_KEY is not configured")
    return key


def _connect_sandbox(sandbox_id: str) -> Sandbox:
    return Sandbox.connect(sandbox_id, api_key=_get_e2b_api_key())


def _parse_ls_output(output: str) -> List[Dict[str, Any]]:
    """Parse `ls -la --time-style=long-iso` output into structured entries."""
    entries = []
    for line in output.strip().splitlines():
        if line.startswith("total "):
            continue
        parts = line.split(None, 8)
        if len(parts) < 9:
            continue
        perms, _, _, _, size_str, _date, _time, name = (
            parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[8] if len(parts) > 8 else parts[7],
        )
        if name in (".", ".."):
            continue
        is_dir = perms.startswith("d")
        entries.append({
            "name": name,
            "type": "dir" if is_dir else "file",
            "size": int(size_str) if not is_dir else 0,
        })
    return entries


async def list_sandbox_files(agent_id: str, path: str, user_jwt: str) -> List[Dict[str, Any]]:
    """List files at the given path inside the agent's running sandbox."""
    supabase = await get_authenticated_async_client(user_jwt)
    result = await (
        supabase.table("agent_instances")
        .select("sandbox_id, sandbox_status")
        .eq("id", agent_id)
        .single()
        .execute()
    )
    agent = result.data
    if not agent or not agent.get("sandbox_id"):
        raise ValueError("Agent has no sandbox")
    if agent.get("sandbox_status") not in ("running", "idle"):
        raise ValueError(f"Sandbox is not running (status: {agent.get('sandbox_status')})")

    safe_path = _sanitize_sandbox_path(path)

    def _run():
        sandbox = _connect_sandbox(agent["sandbox_id"])
        result = sandbox.commands.run(f"ls -la --time-style=long-iso {safe_path}", timeout=10)
        return result.stdout

    output = await asyncio.to_thread(_run)
    return _parse_ls_output(output)


async def read_sandbox_file(agent_id: str, path: str, user_jwt: str) -> str:
    """Read a file from the agent's running sandbox. Max 100KB."""
    supabase = await get_authenticated_async_client(user_jwt)
    result = await (
        supabase.table("agent_instances")
        .select("sandbox_id, sandbox_status")
        .eq("id", agent_id)
        .single()
        .execute()
    )
    agent = result.data
    if not agent or not agent.get("sandbox_id"):
        raise ValueError("Agent has no sandbox")
    if agent.get("sandbox_status") not in ("running", "idle"):
        raise ValueError(f"Sandbox is not running (status: {agent.get('sandbox_status')})")

    safe_path = _sanitize_sandbox_path(path)

    def _run():
        sandbox = _connect_sandbox(agent["sandbox_id"])
        result = sandbox.commands.run(f"head -c 102400 {safe_path}", timeout=10)
        return result.stdout

    return await asyncio.to_thread(_run)
