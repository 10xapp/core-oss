"""
Embedded agent runtime files for upload into E2B sandboxes.

Since core-api deploys on Vercel (serverless), it can't read files from the
core-agent repo at runtime. This module contains all runtime file contents
as string constants so dispatch.py can upload them into new sandboxes.
"""
from typing import Dict


CONFIG_PY = r'''
"""Runtime configuration — reads env vars injected by E2B."""
import os


def _safe_int_env(key: str, default: int) -> int:
    """Parse an integer env var with fallback on bad input."""
    try:
        return int(os.environ.get(key, str(default)))
    except (ValueError, TypeError):
        return default


def _normalize_fail_mode(raw: str) -> str:
    """Normalize TIRITH_FAIL_MODE to 'open' or 'closed'."""
    val = raw.strip().lower()
    if val == "closed":
        return "closed"
    return "open"


class Config:
    AGENT_ID: str = os.environ["AGENT_ID"]
    WORKSPACE_ID: str = os.environ["WORKSPACE_ID"]
    SUPABASE_URL: str = os.environ["SUPABASE_URL"]
    SUPABASE_SERVICE_ROLE_KEY: str = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    ANTHROPIC_API_KEY: str = os.environ["ANTHROPIC_API_KEY"]

    MAX_TURNS: int = int(os.environ.get("MAX_TURNS", "25"))
    MODEL: str = os.environ.get("MODEL", "claude-opus-4-6")
    TASK_POLL_INTERVAL: float = float(os.environ.get("TASK_POLL_INTERVAL", "1.0"))
    IDLE_TIMEOUT_SECONDS: int = int(os.environ.get("IDLE_TIMEOUT", "900"))

    # Sandbox execution
    SANDBOX_CWD: str = os.environ.get("SANDBOX_CWD", "/home/user")

    # Tirith pre-exec scanning
    TIRITH_ENABLED: bool = os.environ.get("TIRITH_ENABLED", "").lower() in ("1", "true", "yes")
    TIRITH_PATH: str = os.environ.get("TIRITH_PATH", "")
    TIRITH_TIMEOUT: int = _safe_int_env("TIRITH_TIMEOUT", 10)
    TIRITH_FAIL_MODE: str = _normalize_fail_mode(os.environ.get("TIRITH_FAIL_MODE", "open"))


config = Config()
'''.lstrip()

STEP_REPORTER_PY = r'''
"""Write agent_task_steps rows to Supabase for real-time observability."""
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Dict

from config import config

logger = logging.getLogger(__name__)


class StepReporter:
    def __init__(self, supabase, task_id: str):
        self.supabase = supabase
        self.task_id = task_id
        self.agent_id = config.AGENT_ID
        self.workspace_id = config.WORKSPACE_ID

    def report(
        self,
        turn: int,
        step_type: str,
        content: Optional[str] = None,
        tool_name: Optional[str] = None,
        tool_args: Optional[Dict[str, Any]] = None,
        tool_result: Optional[Dict[str, Any]] = None,
        token_usage: int = 0,
        duration_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Insert a single step row into agent_task_steps."""
        row = {
            "id": str(uuid.uuid4()),
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "workspace_id": self.workspace_id,
            "turn": turn,
            "step_type": step_type,
            "content": content,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_result": tool_result,
            "token_usage": token_usage,
            "duration_ms": duration_ms,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            result = self.supabase.table("agent_task_steps").insert(row).execute()
            return result.data[0] if result.data else row
        except Exception as e:
            logger.error(f"Failed to report step: {e}")
            return row
'''.lstrip()

TOOLS_PY = r'''
"""Tool definitions for the Anthropic Messages API and their execution logic."""
import subprocess
import os
import json
import logging
from typing import Dict, Any, List, Optional

from config import config

logger = logging.getLogger(__name__)

_TIRITH_MAX_FINDINGS = 10
_TIRITH_MAX_SUMMARY_LEN = 2000
_tirith_warned_reasons: set = set()


def _tirith_warn_once(reason: str, msg: str) -> None:
    """Log a warning at most once per reason key."""
    if reason not in _tirith_warned_reasons:
        logger.warning(msg)
        _tirith_warned_reasons.add(reason)


def _resolve_tirith_bin() -> str | None:
    """Find tirith binary. Returns executable path or None."""
    import shutil

    if config.TIRITH_PATH:
        # Explicit path: try as-is (expand ~), then try shutil.which for PATH-resolved names
        expanded = os.path.expanduser(config.TIRITH_PATH)
        if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
            return expanded
        found = shutil.which(config.TIRITH_PATH)
        if found:
            return found
        return None

    # Auto-detect: PATH first, then well-known default
    found = shutil.which("tirith")
    if found:
        return found
    default = "/usr/local/bin/tirith"
    if os.path.isfile(default) and os.access(default, os.X_OK):
        return default
    return None


def _parse_tirith_findings(stdout: str) -> list:
    """Parse findings from tirith JSON stdout. Returns [] on any parse failure."""
    try:
        verdict = json.loads(stdout)
    except (json.JSONDecodeError, ValueError):
        _tirith_warn_once("bad_json", "tirith stdout not valid JSON, using exit code only")
        return []

    if not isinstance(verdict, dict):
        _tirith_warn_once("bad_json_shape", "tirith JSON was not an object, using exit code only")
        return []

    raw_findings = verdict.get("findings", [])
    if not isinstance(raw_findings, list):
        _tirith_warn_once("bad_findings_shape", "tirith JSON had non-list findings, using exit code only")
        return []

    findings = []
    for f in raw_findings[:_TIRITH_MAX_FINDINGS]:
        if not isinstance(f, dict):
            continue
        entry = {
            "rule_id": f.get("rule_id", "unknown"),
            "severity": f.get("severity", "UNKNOWN"),
            "title": f.get("title", ""),
            "description": f.get("description", ""),
        }
        agent_view = f.get("agent_view")
        if isinstance(agent_view, str) and agent_view:
            entry["agent_view"] = agent_view
        findings.append(entry)
    return findings


def tirith_check_command(command: str) -> dict | None:
    """Run tirith pre-exec scan. Returns None to allow, or error dict to block/warn.

    Exit code is the source of truth:
      0 = allow, 1 = block, 2 = warn.
    JSON stdout enriches with findings but does NOT override the verdict.
    If exit code says block but JSON is missing/malformed, we still block.
    """
    tirith_bin = _resolve_tirith_bin()
    if tirith_bin is None:
        if config.TIRITH_FAIL_MODE == "closed":
            return {"status": "error", "error_type": "security_scan_failed",
                    "message": "Security scanner not available and fail mode is closed."}
        _tirith_warn_once("unavailable", "tirith binary not found, scanning disabled (fail-open)")
        return None

    try:
        proc = subprocess.run(
            [tirith_bin, "check", "--json", "--non-interactive",
             "--shell", "posix", "--", command],
            capture_output=True, text=True, timeout=config.TIRITH_TIMEOUT,
            cwd=config.SANDBOX_CWD,
        )
    except subprocess.TimeoutExpired:
        if config.TIRITH_FAIL_MODE == "closed":
            return {"status": "error", "error_type": "security_scan_failed",
                    "message": "Security scanner timed out."}
        _tirith_warn_once("timeout", f"tirith check timed out after {config.TIRITH_TIMEOUT}s, fail-open")
        return None
    except OSError as exc:
        if config.TIRITH_FAIL_MODE == "closed":
            return {"status": "error", "error_type": "security_scan_failed",
                    "message": f"Security scanner OS error: {exc}"}
        _tirith_warn_once("oserror", f"tirith check OS error: {exc}, fail-open")
        return None

    # Exit code 0 = allow
    if proc.returncode == 0:
        return None

    # Exit code 1 = block, 2 = warn — these are authoritative
    if proc.returncode in (1, 2):
        action = "block" if proc.returncode == 1 else "warn"
        findings = _parse_tirith_findings(proc.stdout)

        summary = "; ".join(
            f"[{f['severity']}] {f['title']}" for f in findings
        )[:_TIRITH_MAX_SUMMARY_LEN]

        if summary:
            msg = (f"Command {'blocked' if action == 'block' else 'flagged'} "
                   f"by security scan: {summary}. "
                   "Review the findings and reformulate the command.")
        else:
            msg = (f"Command {'blocked' if action == 'block' else 'flagged'} "
                   "by security scan (details unavailable). "
                   "Do not retry the same command.")

        return {
            "status": "error",
            "error_type": "security_blocked",
            "tirith_action": action,
            "message": msg,
            "findings": findings,
        }

    # Unknown exit code — not a security verdict
    if config.TIRITH_FAIL_MODE == "closed":
        return {"status": "error", "error_type": "security_scan_failed",
                "message": f"Security scanner exited with unexpected code {proc.returncode}."}
    _tirith_warn_once("unknown_exit", f"tirith unexpected exit code {proc.returncode}, fail-open")
    return None


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "bash",
        "description": "Run a bash command in the sandbox. Returns stdout and stderr.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The bash command to execute",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "read_file",
        "description": "Read a file from the sandbox filesystem.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to read",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Write content to a file in the sandbox.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to write to",
                },
                "content": {
                    "type": "string",
                    "description": "Content to write to the file",
                },
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "edit_file",
        "description": (
            "Edit a file by replacing an exact string match with new content. "
            "The old_string must match EXACTLY (including whitespace). "
            "Only the first occurrence is replaced. "
            "To delete text, use new_string=''. To insert at beginning, use old_string=''."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to edit",
                },
                "old_string": {
                    "type": "string",
                    "description": "The exact string to find and replace",
                },
                "new_string": {
                    "type": "string",
                    "description": "The replacement string",
                },
            },
            "required": ["path", "old_string", "new_string"],
        },
    },
    {
        "name": "send_channel_message",
        "description": "Send a message to a workspace channel on behalf of this agent.",
        "input_schema": {
            "type": "object",
            "properties": {
                "channel_id": {
                    "type": "string",
                    "description": "The channel ID to send the message to",
                },
                "content": {
                    "type": "string",
                    "description": "The message content to send",
                },
            },
            "required": ["channel_id", "content"],
        },
    },
    {
        "name": "refresh_workspace",
        "description": "Force refresh all workspace data files in /home/user/context/workspace/. Use if data seems stale or you need the latest information.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
]

# Tools that need special handling
WORKSPACE_TOOLS = {"send_channel_message", "refresh_workspace"}


def execute_tool(name: str, args: Dict[str, Any], supabase=None, workspace_sync=None) -> Dict[str, Any]:
    """Execute a tool and return a result dict."""
    try:
        if name == "bash":
            if config.TIRITH_ENABLED:
                scan = tirith_check_command(args["command"])
                if scan is not None:
                    return scan
            result = subprocess.run(
                args["command"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=config.SANDBOX_CWD,
            )
            output = result.stdout + result.stderr
            return {
                "status": "ok" if result.returncode == 0 else "error",
                "exit_code": result.returncode,
                "output": output[:10000],
            }

        elif name == "read_file":
            path = args["path"]
            if not os.path.exists(path):
                return {"status": "error", "message": f"File not found: {path}"}
            with open(path, "r", errors="replace") as f:
                content = f.read(50000)
            return {"status": "ok", "content": content}

        elif name == "write_file":
            path = args["path"]
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(args["content"])
            return {"status": "ok", "message": f"Written {len(args['content'])} chars to {path}"}

        elif name == "edit_file":
            path = args["path"]
            old_string = args["old_string"]
            new_string = args["new_string"]
            if not os.path.exists(path):
                return {"status": "error", "message": f"File not found: {path}"}
            with open(path, "r", errors="replace") as f:
                content = f.read()
            if old_string == "":
                new_content = new_string + content
            elif old_string not in content:
                return {"status": "error", "message": f"old_string not found in {path}"}
            else:
                new_content = content.replace(old_string, new_string, 1)
            with open(path, "w") as f:
                f.write(new_content)
            return {"status": "ok", "message": f"Edited {path}"}

        elif name == "send_channel_message":
            if not supabase:
                return {"status": "error", "message": "Supabase client not available"}
            channel_id = args["channel_id"]
            content = args["content"]
            result = (
                supabase.table("channel_messages")
                .insert({
                    "channel_id": channel_id,
                    "content": content,
                    "user_id": config.AGENT_ID,
                })
                .execute()
            )
            msg = result.data[0] if result.data else {}
            return {"status": "ok", "message_id": msg.get("id"), "message": "Message sent"}

        elif name == "refresh_workspace":
            if not workspace_sync:
                return {"status": "error", "message": "Workspace sync not available"}
            error = workspace_sync.full_sync()
            if error:
                return {"status": "error", "message": f"Workspace refresh failed: {error}"}
            return {"status": "ok", "message": "Workspace data refreshed"}

        else:
            return {"status": "error", "message": f"Unknown tool: {name}"}

    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Command timed out after 30 seconds"}
    except Exception as e:
        logger.error(f"Tool '{name}' execution failed: {e}")
        return {"status": "error", "message": str(e)}
'''.lstrip()

AGENT_LOOP_PY = r'''
"""Claude API tool-calling loop using Anthropic Messages API."""
import json
import time
import logging
from typing import Dict, Any, Optional

import anthropic

from config import config
from tools import TOOL_DEFINITIONS, execute_tool
from step_reporter import StepReporter

logger = logging.getLogger(__name__)


def _load_conversation_history(supabase, conversation_id: str, current_task_id: str, max_pairs: int = 20):
    """Load previous completed tasks from the same conversation as message history."""
    result = (
        supabase.table("agent_tasks")
        .select("input, output")
        .eq("conversation_id", conversation_id)
        .eq("status", "completed")
        .neq("id", current_task_id)
        .order("created_at")
        .limit(max_pairs)
        .execute()
    )

    messages = []
    for task in (result.data or []):
        instruction = (task.get("input") or {}).get("instruction", "")
        response = (task.get("output") or {}).get("response", "")
        if instruction:
            messages.append({"role": "user", "content": instruction})
        if response:
            messages.append({"role": "assistant", "content": response})
    return messages


def run_agent_loop(
    supabase,
    task: Dict[str, Any],
    system_prompt: str,
    model: Optional[str] = None,
    workspace_sync=None,
) -> Dict[str, Any]:
    """
    Run the Claude tool-calling loop for a task.

    Returns:
        {"response": str, "turns": int, "total_tokens": int}
    """
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    reporter = StepReporter(supabase, task["id"])
    model = model or config.MODEL
    total_tokens = 0

    # Load conversation history if this task belongs to a conversation
    messages = []
    conversation_id = task.get("conversation_id")
    if conversation_id:
        messages = _load_conversation_history(supabase, conversation_id, task["id"])
    messages.append({"role": "user", "content": task["instruction"]})

    for turn in range(1, config.MAX_TURNS + 1):
        t0 = time.time()
        try:
            response = client.messages.create(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )
        except Exception as e:
            reporter.report(turn=turn, step_type="error", content=f"Claude API error: {e}")
            raise

        elapsed_ms = int((time.time() - t0) * 1000)
        turn_tokens = response.usage.input_tokens + response.usage.output_tokens
        total_tokens += turn_tokens

        # Separate text and tool_use blocks
        text_parts = []
        tool_use_blocks = []

        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_use_blocks.append(block)

        # Report text content
        if text_parts:
            combined_text = "\n".join(text_parts)
            is_final = response.stop_reason == "end_turn" and not tool_use_blocks
            reporter.report(
                turn=turn,
                step_type="message" if is_final else "thinking",
                content=combined_text,
                token_usage=turn_tokens,
                duration_ms=elapsed_ms,
            )

        # Done if no tool calls
        if response.stop_reason == "end_turn" and not tool_use_blocks:
            return {
                "response": "\n".join(text_parts) if text_parts else "",
                "turns": turn,
                "total_tokens": total_tokens,
            }

        # Add assistant message to conversation
        messages.append({"role": "assistant", "content": response.content})

        # Execute each tool call
        tool_results = []
        for tool_block in tool_use_blocks:
            tool_name = tool_block.name
            tool_input = tool_block.input

            reporter.report(
                turn=turn,
                step_type="tool_call",
                tool_name=tool_name,
                tool_args=tool_input,
            )

            t_tool = time.time()
            result = execute_tool(tool_name, tool_input, supabase=supabase, workspace_sync=workspace_sync)
            tool_elapsed = int((time.time() - t_tool) * 1000)

            reporter.report(
                turn=turn,
                step_type="tool_result",
                tool_name=tool_name,
                tool_result=result,
                duration_ms=tool_elapsed,
            )

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_block.id,
                "content": json.dumps(result),
            })

        messages.append({"role": "user", "content": tool_results})

    # Exceeded max turns
    reporter.report(
        turn=config.MAX_TURNS,
        step_type="error",
        content=f"Reached max turns ({config.MAX_TURNS})",
    )
    return {
        "response": f"[Reached maximum of {config.MAX_TURNS} turns]",
        "turns": config.MAX_TURNS,
        "total_tokens": total_tokens,
    }
'''.lstrip()

TASK_WATCHER_PY = r'''
"""Watch for new tasks via Supabase database polling.

Previously watched /tmp/tasks/ for JSON files injected by the API via E2B.
Now polls the agent_tasks table directly, eliminating the need for
Sandbox.connect() on the dispatch side (saves ~5-12s per message).
"""
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class TaskWatcher:
    def __init__(self, supabase, agent_id: str):
        self.supabase = supabase
        self.agent_id = agent_id

    def poll(self) -> Optional[Dict[str, Any]]:
        """Check for new queued tasks in the database. Returns the oldest queued task, or None."""
        try:
            result = (
                self.supabase.table("agent_tasks")
                .select("*")
                .eq("agent_id", self.agent_id)
                .eq("status", "queued")
                .order("created_at")
                .limit(1)
                .execute()
            )

            if not result.data:
                return None

            row = result.data[0]

            # Build task dict matching the format agent_loop expects
            task_input = row.get("input", {})
            task = {
                "id": row["id"],
                "agent_id": row["agent_id"],
                "workspace_id": row["workspace_id"],
                "instruction": task_input.get("instruction", ""),
                "channel_id": task_input.get("channel_id"),
                "invoked_by": task_input.get("invoked_by"),
                "conversation_id": row.get("conversation_id"),
                "created_at": row["created_at"],
            }

            logger.info(f"Picked up task from DB: {task['id']}")
            return task

        except Exception as e:
            logger.error(f"Failed to poll for tasks: {e}")
            return None
'''.lstrip()

MAIN_PY = r'''
"""
Core Agent Runtime — Entry point.
Runs inside an E2B sandbox. Watches for task files, runs Claude agent loops,
reports steps back to Supabase in real-time.
"""
import os
import json
import time
import logging
import sys
import threading
from datetime import datetime, timezone

from supabase import create_client

from config import config
from task_watcher import TaskWatcher
from agent_loop import run_agent_loop
from workspace_sync import WorkspaceSync


CONTEXT = "/home/user/context"
WS = "/home/user/context/workspace"
PERSONAL = "/home/user/context/personal"


def build_workspace_prompt(app_types: set) -> str:
    """Build workspace context prompt based on which apps exist."""
    lines = [
        "",
        "## Workspace Context",
        "",
        f"Your workspace data is available as plain-text files under {WS}/.",
        "This data refreshes automatically every 30 seconds.",
        "",
        "Files available:",
        f"- {WS}/team.txt — Team members and roles",
    ]
    if "messages" in app_types:
        lines.append(f"- {WS}/channels/_index.txt — List of channels (with channel_ids)")
        lines.append(f"- {WS}/channels/{{name}}.txt — Recent messages in each channel")
    if "files" in app_types:
        lines.append(f"- {WS}/documents/_index.txt — List of documents (with doc_ids)")
        lines.append(f"- {WS}/documents/{{id}}.txt — Individual document content")
    if "projects" in app_types:
        lines.append(f"- {WS}/projects/_index.txt — List of project boards")
        lines.append(f"- {WS}/projects/{{name}}.txt — Board with issues grouped by state")
    if "tasks" in app_types:
        lines.append(f"- {WS}/todos.txt — Todos grouped by team member")
    lines.append("")
    lines.append("To read any of this data, use the read_file tool.")
    if "messages" in app_types:
        lines.append("To send a message to a channel, use the send_channel_message tool with the channel_id from the channel file header.")
    lines.append("To force-refresh all data, use the refresh_workspace tool.")
    return "\n".join(lines)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("core-agent")


def get_supabase():
    return create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_ROLE_KEY)


def fetch_agent_config(supabase) -> dict:
    result = (
        supabase.table("agent_instances")
        .select("*")
        .eq("id", config.AGENT_ID)
        .single()
        .execute()
    )
    return result.data


def update_task_status(supabase, task_id: str, status: str, extra: dict = None):
    update = {"status": status}
    if status == "running":
        update["started_at"] = datetime.now(timezone.utc).isoformat()
    if status in ("completed", "failed"):
        update["completed_at"] = datetime.now(timezone.utc).isoformat()
    if extra:
        update.update(extra)
    supabase.table("agent_tasks").update(update).eq("id", task_id).execute()


def update_agent_status(supabase, status: str):
    supabase.table("agent_instances").update({
        "status": status,
        "last_active_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", config.AGENT_ID).execute()


def _restore_storage_dir(supabase, storage_prefix, local_dir):
    """Recursively restore files from Supabase Storage to a local directory."""
    try:
        items = supabase.storage.from_("agent-data").list(storage_prefix)
        if not items:
            return
        for item in items:
            name = item.get("name", "")
            if not name or name == ".emptyFolderPlaceholder":
                continue
            storage_path = f"{storage_prefix}/{name}"
            local_path = os.path.join(local_dir, name)
            if item.get("id") is None:
                os.makedirs(local_path, exist_ok=True)
                _restore_storage_dir(supabase, storage_path, local_path)
            else:
                try:
                    data = supabase.storage.from_("agent-data").download(storage_path)
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    with open(local_path, "wb") as f:
                        f.write(data)
                    logger.info(f"Restored {storage_path}")
                except Exception as e:
                    logger.warning(f"Failed to restore {storage_path}: {e}")
    except Exception as e:
        logger.warning(f"Failed to list {storage_prefix}: {e}")


def restore_personal_from_storage(supabase, agent_id):
    """On boot, restore personal files from Supabase Storage backup.
    Only restores personal/ subfolder — workspace data comes from WorkspaceSync."""
    os.makedirs(PERSONAL, exist_ok=True)
    _restore_storage_dir(supabase, f"{agent_id}/personal", PERSONAL)


def sync_context_to_storage(supabase, agent_id):
    """After each task, sync /home/user/context/ to Supabase Storage backup."""
    if not os.path.exists(CONTEXT):
        return
    for root, _dirs, files in os.walk(CONTEXT):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, CONTEXT)
            storage_path = f"{agent_id}/{relative_path}"
            try:
                with open(local_path, "rb") as f:
                    content = f.read()
                try:
                    supabase.storage.from_("agent-data").update(storage_path, content)
                except Exception:
                    supabase.storage.from_("agent-data").upload(storage_path, content)
            except Exception as e:
                logger.warning(f"Failed to sync {relative_path}: {e}")


def load_personal_context():
    """List agent's local personal files for system prompt context."""
    try:
        os.makedirs(PERSONAL, exist_ok=True)
        files = []
        for root, _dirs, filenames in os.walk(PERSONAL):
            for fn in filenames:
                rel = os.path.relpath(os.path.join(root, fn), PERSONAL)
                files.append(rel)
        ctx = (
            "\n\n## Your File System\n\n"
            "Your home directory is /home/user/. Here is how it is organized:\n"
            f"- {WS}/ — Workspace data (read-only, auto-synced). Do NOT write files here.\n"
            f"- {PERSONAL}/ — YOUR personal files. ALWAYS save your own files here.\n"
            "\n"
            "IMPORTANT: Any files you create (notes, research, summaries, memories, etc.) "
            f"MUST go in {PERSONAL}/. Files in this directory are automatically backed up "
            "and persist across sandbox restarts. Do NOT write files to /home/user/ directly.\n"
        )
        if files:
            ctx += "\nCurrent personal files: " + ", ".join(files) + "\n"
        else:
            ctx += f"\nYour personal directory is empty. Start saving files to {PERSONAL}/ as you work.\n"
        return ctx
    except Exception as e:
        logger.warning(f"Failed to load personal context: {e}")
        return ""


IDENTITY_PATH = os.path.join(PERSONAL, "identity.json")
MEMORIES_PATH = os.path.join(PERSONAL, "memories.md")


def load_identity() -> dict:
    """Load identity.json from personal folder if it exists."""
    if not os.path.exists(IDENTITY_PATH):
        return {}
    try:
        with open(IDENTITY_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load identity.json: {e}")
        return {}


def build_identity_prompt(identity: dict) -> str:
    """Build a rich system prompt from identity.json fields."""
    if not identity:
        return ""
    parts = []
    name = identity.get("name", "")
    role = identity.get("role", "")
    if name and role:
        parts.append(f"You are {name}, {role}.")
    elif name:
        parts.append(f"You are {name}.")
    if identity.get("backstory"):
        parts.append(f"\n{identity['backstory']}")
    if identity.get("objective"):
        parts.append(f"\nYour objective: {identity['objective']}")
    if identity.get("personality"):
        parts.append(f"\nYour communication style is {identity['personality']}.")
    parts.append(
        "\nYou are an AI employee at this company. "
        "Be helpful, proactive, and professional."
    )
    return "\n".join(parts)


def extract_memories(instruction: str, response: str):
    """Use Haiku to update memories.md after a completed task."""
    import anthropic

    existing = ""
    if os.path.exists(MEMORIES_PATH):
        try:
            with open(MEMORIES_PATH, "r") as f:
                existing = f.read()
        except Exception as e:
            logger.warning(f"Failed to read memories.md: {e}")

    try:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        result = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{"role": "user", "content": (
                "You manage a memories file for an AI agent. Given the conversation "
                "below and existing memories, return the updated memories file.\n\n"
                "Rules:\n"
                "- Use markdown with ## category headers (Facts, People, Preferences, Processes, etc.)\n"
                "- Each memory is a bullet point (- )\n"
                "- Add new info learned from the conversation\n"
                "- Remove or update entries contradicted by new info\n"
                "- Keep entries concise (one line each)\n"
                "- If nothing new was learned, return existing content unchanged\n"
                "- Do NOT add trivial information\n"
                "- Max 50 entries total\n\n"
                f"EXISTING MEMORIES:\n{existing if existing else '(empty)'}\n\n"
                f"TASK: {instruction}\n"
                f"RESPONSE: {response[:2000]}\n\n"
                "Return ONLY the updated memories.md content:"
            )}],
        )
        new_memories = result.content[0].text.strip()
        if new_memories:
            os.makedirs(os.path.dirname(MEMORIES_PATH), exist_ok=True)
            with open(MEMORIES_PATH, "w") as f:
                f.write(new_memories + "\n")
            logger.info(f"Memories updated ({len(new_memories)} chars)")
        else:
            logger.info("No memory updates from extraction")
    except Exception as e:
        logger.warning(f"Memory extraction failed: {e}")


def main():
    boot_start = time.perf_counter()
    logger.info(f"Core Agent Runtime starting (agent_id={config.AGENT_ID})")

    t0 = time.perf_counter()
    supabase = get_supabase()
    logger.info(f"Supabase client ready ({int((time.perf_counter() - t0) * 1000)}ms)")

    watcher = TaskWatcher(supabase, config.AGENT_ID)

    t0 = time.perf_counter()
    agent = fetch_agent_config(supabase)
    if not agent:
        logger.error(f"Agent {config.AGENT_ID} not found in database")
        sys.exit(1)
    logger.info(f"Agent config fetched ({int((time.perf_counter() - t0) * 1000)}ms)")

    # Populate /workspace/ with plain-text workspace data
    t0 = time.perf_counter()
    ws_sync = WorkspaceSync(supabase, config.WORKSPACE_ID)
    sync_error = ws_sync.full_sync()
    if sync_error:
        logger.warning(f"Workspace sync failed: {sync_error}")
    ws_sync.start_polling(interval=30)
    logger.info(f"Workspace sync {'failed' if sync_error else 'complete'} ({int((time.perf_counter() - t0) * 1000)}ms)")

    # Restore personal files from Supabase Storage backup
    t0 = time.perf_counter()
    restore_personal_from_storage(supabase, config.AGENT_ID)
    logger.info(f"Personal files restored ({int((time.perf_counter() - t0) * 1000)}ms)")

    identity = load_identity()
    if identity:
        base_prompt = build_identity_prompt(identity) + build_workspace_prompt(ws_sync.app_types)
        logger.info(f"Using identity-based prompt for '{identity.get('name', agent['name'])}'")
    else:
        base_prompt = (agent.get("system_prompt") or "") + build_workspace_prompt(ws_sync.app_types)
    model = agent.get("config", {}).get("model", config.MODEL)

    total_boot_ms = int((time.perf_counter() - boot_start) * 1000)
    logger.info(f"Agent '{agent['name']}' ready. Model: {model}. Boot: {total_boot_ms}ms. Watching for tasks...")

    update_agent_status(supabase, "idle")

    last_activity = time.time()

    while True:
        task = watcher.poll()

        if task:
            last_activity = time.time()
            task_id = task["id"]
            logger.info(f"Processing task {task_id}: {task.get('instruction', '')[:100]}")

            # Set running status immediately so the UI responds fast
            update_task_status(supabase, task_id, "running")
            update_agent_status(supabase, "working")

            # Refresh workspace data in background (non-blocking)
            ws_sync.refresh_if_stale(max_age_seconds=30, blocking=False)

            # Build effective prompt with current personal file context
            personal_ctx = load_personal_context()
            effective_prompt = base_prompt + personal_ctx

            try:
                result = run_agent_loop(
                    supabase=supabase,
                    task=task,
                    system_prompt=effective_prompt,
                    model=model,
                    workspace_sync=ws_sync,
                )

                # Extract memories from conversation (synchronous — Haiku is fast)
                try:
                    extract_memories(
                        task.get("instruction", ""),
                        result["response"],
                    )
                except Exception as mem_err:
                    logger.warning(f"Memory extraction failed (non-fatal): {mem_err}")

                # Sync context to Supabase Storage backup (non-blocking)
                threading.Thread(
                    target=sync_context_to_storage,
                    args=(supabase, config.AGENT_ID),
                    daemon=True,
                ).start()

                update_task_status(supabase, task_id, "completed", extra={
                    "output": {"response": result["response"], "turns": result["turns"]},
                    "token_usage": result["total_tokens"],
                })
                logger.info(
                    f"Task {task_id} completed "
                    f"({result['turns']} turns, {result['total_tokens']} tokens)"
                )

            except Exception as e:
                logger.error(f"Task {task_id} failed: {e}", exc_info=True)
                update_task_status(supabase, task_id, "failed", extra={
                    "error": str(e),
                })

            finally:
                update_agent_status(supabase, "idle")

        else:
            idle_seconds = time.time() - last_activity
            if idle_seconds > config.IDLE_TIMEOUT_SECONDS:
                logger.info(f"Idle for {idle_seconds:.0f}s — exiting.")
                update_agent_status(supabase, "idle")
                break

        time.sleep(config.TASK_POLL_INTERVAL)


if __name__ == "__main__":
    main()

'''.lstrip()

WORKSPACE_SYNC_PY = r'''
"""
Workspace data sync — fetches workspace data from Supabase and writes
human-readable plain-text files to /workspace/ in the sandbox filesystem.

Data is rendered as natural English so the agent can read it like a human would.
"""
import os
import re
import time
import threading
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from config import config

logger = logging.getLogger(__name__)

WORKSPACE_DIR = "/home/user/context/workspace"


def _slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "untitled"


def _format_date(iso_str: Optional[str]) -> str:
    """Format ISO timestamp to human-readable like 'Feb 23, 10:45am'."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %I:%M%p").replace(" 0", " ").lower().replace("am", "am").replace("pm", "pm")
    except Exception:
        return iso_str[:10] if iso_str else ""


def _format_date_short(iso_str: Optional[str]) -> str:
    """Format ISO timestamp to short date like 'Feb 23'."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%b %d").replace(" 0", " ")
    except Exception:
        return iso_str[:10] if iso_str else ""


def _atomic_write(path: str, content: str):
    """Write file atomically — write to tmp then rename."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(content)
    os.rename(tmp_path, path)


class WorkspaceSync:
    """Fetches workspace data from Supabase and writes plain-text files."""

    def __init__(self, supabase, workspace_id: str):
        self.supabase = supabase
        self.workspace_id = workspace_id
        self.workspace_app_ids: List[str] = []
        self.app_types: set = set()  # e.g. {"messages", "files", "projects", "tasks"}
        self.user_names: Dict[str, str] = {}  # user_id -> full_name
        self.user_emails: Dict[str, str] = {}  # user_id -> email
        self.user_roles: Dict[str, str] = {}  # user_id -> role
        self._last_synced: float = 0
        self._polling_thread: Optional[threading.Thread] = None
        self._running = False

    def full_sync(self) -> Optional[str]:
        """Fetch all workspace data and write plain-text files to /home/user/workspace/.
        Only syncs data for mini-apps that exist in the workspace.
        Returns None on success, error string with details on partial/full failure."""
        os.makedirs(WORKSPACE_DIR, exist_ok=True)
        errors = []

        try:
            self._fetch_workspace_app_ids()
        except Exception as e:
            errors.append(f"workspace_apps: {e}")
            logger.error(f"Failed to fetch workspace app ids: {e}")

        try:
            self._sync_members()
        except Exception as e:
            errors.append(f"members: {e}")
            logger.error(f"Failed to sync members: {e}")

        # Map app_type -> sync function
        app_syncs = {
            "messages": ("channels", self._sync_channels),
            "files": ("documents", self._sync_documents),
            "projects": ("projects", self._sync_projects),
            "tasks": ("todos", self._sync_todos),
        }
        for app_type, (name, fn) in app_syncs.items():
            if app_type not in self.app_types:
                logger.info(f"Skipping {name} sync (no '{app_type}' app in workspace)")
                continue
            try:
                fn()
            except Exception as e:
                errors.append(f"{name}: {e}")
                logger.error(f"Failed to sync {name}: {e}")

        self._last_synced = time.time()
        if errors:
            msg = "; ".join(errors)
            logger.warning(f"Workspace sync completed with errors: {msg}")
            return msg
        logger.info(f"Workspace sync complete (apps: {', '.join(sorted(self.app_types))})")
        return None

    def start_polling(self, interval: int = 30):
        """Start background daemon thread that re-syncs every N seconds."""
        self._running = True
        self._polling_thread = threading.Thread(
            target=self._polling_loop,
            args=(interval,),
            daemon=True,
            name="workspace-sync",
        )
        self._polling_thread.start()
        logger.info(f"Workspace polling started (every {interval}s)")

    def stop(self):
        self._running = False

    def refresh_if_stale(self, max_age_seconds: int = 30, blocking: bool = True):
        """Re-sync if data is older than max_age_seconds.
        If blocking=False, runs sync in a background thread."""
        age = time.time() - self._last_synced
        if age > max_age_seconds:
            if blocking:
                logger.info(f"Workspace data is {age:.0f}s old, refreshing...")
                self.full_sync()
            else:
                logger.info(f"Workspace data is {age:.0f}s old, refreshing in background...")
                threading.Thread(target=self.full_sync, daemon=True).start()

    def _resolve_name(self, user_id: Optional[str]) -> str:
        """Return human name for a user_id."""
        if not user_id:
            return "Unknown"
        return self.user_names.get(user_id, "Unknown")

    # =========================================================================
    # Internal: fetch workspace_app_ids
    # =========================================================================

    def _fetch_workspace_app_ids(self):
        result = (
            self.supabase.table("workspace_apps")
            .select("id, app_type")
            .eq("workspace_id", self.workspace_id)
            .execute()
        )
        rows = result.data or []
        self.workspace_app_ids = [r["id"] for r in rows]
        self.app_types = {r["app_type"] for r in rows}
        logger.info(f"Workspace apps: {sorted(self.app_types)}")

    # =========================================================================
    # Sync: Members -> /workspace/team.txt
    # =========================================================================

    def _sync_members(self):
        # Fetch workspace members
        members_result = (
            self.supabase.table("workspace_members")
            .select("user_id, role")
            .eq("workspace_id", self.workspace_id)
            .execute()
        )
        members = members_result.data or []
        user_ids = [m["user_id"] for m in members]

        # Fetch user details from users table
        users_by_id = {}
        if user_ids:
            users_result = (
                self.supabase.table("users")
                .select("id, name, email, avatar_url")
                .in_("id", user_ids)
                .execute()
            )
            for u in (users_result.data or []):
                users_by_id[u["id"]] = u

        # Build lookup tables
        self.user_names = {}
        self.user_emails = {}
        self.user_roles = {}
        for m in members:
            uid = m["user_id"]
            user = users_by_id.get(uid, {})
            self.user_names[uid] = user.get("name") or "Unknown"
            self.user_emails[uid] = user.get("email") or ""
            self.user_roles[uid] = m.get("role", "member")

        # Render
        lines = ["Workspace Team", "==============", ""]
        for m in members:
            uid = m["user_id"]
            name = self.user_names[uid]
            email = self.user_emails[uid]
            role = self.user_roles[uid].capitalize()
            lines.append(f"- {name} ({email}) -- {role}")

        _atomic_write(os.path.join(WORKSPACE_DIR, "team.txt"), "\n".join(lines) + "\n")

    # =========================================================================
    # Sync: Channels -> /workspace/channels/_index.txt + per-channel .txt
    # =========================================================================

    def _sync_channels(self):
        if not self.workspace_app_ids:
            return

        result = (
            self.supabase.table("channels")
            .select("id, name, description, is_dm, workspace_app_id")
            .in_("workspace_app_id", self.workspace_app_ids)
            .eq("is_dm", False)
            .order("name")
            .execute()
        )
        channels = result.data or []

        # Write index
        index_lines = ["Channels", "========", ""]
        for ch in channels:
            desc = ch.get("description") or "No description"
            index_lines.append(f"- #{ch['name']} (channel_id: {ch['id']}) -- {desc}")
        channels_dir = os.path.join(WORKSPACE_DIR, "channels")
        os.makedirs(channels_dir, exist_ok=True)
        _atomic_write(os.path.join(channels_dir, "_index.txt"), "\n".join(index_lines) + "\n")

        # Write per-channel messages
        for ch in channels:
            self._sync_channel_messages(ch)

    def _sync_channel_messages(self, channel: Dict[str, Any]):
        channel_id = channel["id"]
        channel_name = channel["name"]

        result = (
            self.supabase.table("channel_messages")
            .select("id, content, user_id, created_at")
            .eq("channel_id", channel_id)
            .order("created_at", desc=True)
            .limit(100)
            .execute()
        )
        messages = list(reversed(result.data or []))

        header = f"#{channel_name} (channel_id: {channel_id})"
        lines = [header, "=" * len(header), ""]

        if not messages:
            lines.append("No messages yet.")
        else:
            for msg in messages:
                name = self._resolve_name(msg.get("user_id"))
                ts = _format_date(msg.get("created_at"))
                content = (msg.get("content") or "").strip()
                if content:
                    lines.append(f"{name} ({ts}): {content}")

        slug = _slugify(channel_name)
        channels_dir = os.path.join(WORKSPACE_DIR, "channels")
        _atomic_write(os.path.join(channels_dir, f"{slug}.txt"), "\n".join(lines) + "\n")

    # =========================================================================
    # Sync: Documents -> /workspace/documents/_index.txt + per-doc .txt
    # =========================================================================

    def _sync_documents(self):
        if not self.workspace_app_ids:
            return

        result = (
            self.supabase.table("documents")
            .select("id, title, content, user_id, created_at, updated_at")
            .in_("workspace_app_id", self.workspace_app_ids)
            .eq("is_folder", False)
            .order("updated_at", desc=True)
            .limit(50)
            .execute()
        )
        docs = result.data or []

        # Write index
        index_lines = ["Documents", "=========", ""]
        for doc in docs:
            title = doc.get("title") or "Untitled"
            updated = _format_date_short(doc.get("updated_at"))
            author = self._resolve_name(doc.get("user_id"))
            index_lines.append(f"- {title} (doc_id: {doc['id']}) -- Updated {updated} by {author}")

        docs_dir = os.path.join(WORKSPACE_DIR, "documents")
        os.makedirs(docs_dir, exist_ok=True)
        _atomic_write(os.path.join(docs_dir, "_index.txt"), "\n".join(index_lines) + "\n")

        # Write individual documents
        for doc in docs:
            title = doc.get("title") or "Untitled"
            author = self._resolve_name(doc.get("user_id"))
            updated = _format_date_short(doc.get("updated_at"))
            content = doc.get("content") or ""

            if len(content) > 10000:
                content = content[:10000] + f"\n\n... (truncated, full length: {len(doc.get('content', ''))} chars)"

            lines = [
                title,
                "=" * len(title),
                f"By {author} | Updated {updated}",
                "",
                content,
            ]

            _atomic_write(
                os.path.join(docs_dir, f"{doc['id']}.txt"),
                "\n".join(lines) + "\n",
            )

    # =========================================================================
    # Sync: Projects -> /workspace/projects/_index.txt + per-board .txt
    # =========================================================================

    def _sync_projects(self):
        boards_result = (
            self.supabase.table("project_boards")
            .select("id, name, description, key")
            .eq("workspace_id", self.workspace_id)
            .order("position")
            .execute()
        )
        boards = boards_result.data or []

        projects_dir = os.path.join(WORKSPACE_DIR, "projects")
        os.makedirs(projects_dir, exist_ok=True)

        index_lines = ["Project Boards", "==============", ""]

        for board in boards:
            board_id = board["id"]

            # Fetch states for this board
            states_result = (
                self.supabase.table("project_states")
                .select("id, name, position")
                .eq("board_id", board_id)
                .order("position")
                .execute()
            )
            states = states_result.data or []

            # Fetch issues for this board
            issues_result = (
                self.supabase.table("project_issues")
                .select("id, number, title, description, priority, state_id, assignee_id, due_at, completed_at, position")
                .eq("board_id", board_id)
                .order("position")
                .limit(100)
                .execute()
            )
            issues = issues_result.data or []

            # Group issues by state
            issues_by_state: Dict[str, List] = {s["id"]: [] for s in states}
            for issue in issues:
                sid = issue.get("state_id")
                if sid in issues_by_state:
                    issues_by_state[sid].append(issue)

            # Count for index
            open_count = sum(1 for i in issues if not i.get("completed_at"))
            done_count = sum(1 for i in issues if i.get("completed_at"))

            board_name = board.get("name") or "Untitled Board"
            index_lines.append(
                f"- {board_name} (board_id: {board_id}) -- {open_count} open, {done_count} completed"
            )

            # Write per-board file
            header = f"{board_name} (board_id: {board_id})"
            board_lines = [header, "=" * len(header), ""]

            for state in states:
                state_issues = issues_by_state.get(state["id"], [])
                board_lines.append(f"## {state['name']}")

                if not state_issues:
                    board_lines.append("  (empty)")
                else:
                    for issue in state_issues:
                        num = issue.get("number", "?")
                        title = issue.get("title", "Untitled")
                        priority_map = {0: "None", 1: "Low", 2: "Medium", 3: "High", 4: "Urgent"}
                        priority = priority_map.get(issue.get("priority", 0), "None")
                        assignee = self._resolve_name(issue.get("assignee_id"))
                        parts = [f"#{num} {title}"]
                        parts.append(f"Priority: {priority}")
                        if issue.get("assignee_id"):
                            parts.append(f"Assigned: {assignee}")
                        else:
                            parts.append("Unassigned")
                        if issue.get("completed_at"):
                            parts.append(f"Completed {_format_date_short(issue['completed_at'])}")
                        elif issue.get("due_at"):
                            parts.append(f"Due: {_format_date_short(issue['due_at'])}")
                        board_lines.append(f"- {parts[0]} ({', '.join(parts[1:])})")

                board_lines.append("")

            slug = _slugify(board_name)
            _atomic_write(os.path.join(projects_dir, f"{slug}.txt"), "\n".join(board_lines) + "\n")

        _atomic_write(os.path.join(projects_dir, "_index.txt"), "\n".join(index_lines) + "\n")

    # =========================================================================
    # Sync: Todos -> /workspace/todos.txt
    # =========================================================================

    def _sync_todos(self):
        user_ids = list(self.user_names.keys())
        if not user_ids:
            _atomic_write(os.path.join(WORKSPACE_DIR, "todos.txt"), "Todos\n=====\n\nNo team members found.\n")
            return

        result = (
            self.supabase.table("todos")
            .select("id, title, notes, is_completed, priority, due_at, user_id, completed_at")
            .in_("user_id", user_ids)
            .order("created_at", desc=True)
            .limit(200)
            .execute()
        )
        todos = result.data or []

        # Group by user
        todos_by_user: Dict[str, List] = {}
        for t in todos:
            uid = t.get("user_id", "")
            todos_by_user.setdefault(uid, []).append(t)

        lines = ["Todos", "=====", ""]

        for uid, name in sorted(self.user_names.items(), key=lambda x: x[1]):
            user_todos = todos_by_user.get(uid, [])
            if not user_todos:
                continue

            lines.append(f"{name}:")
            for t in user_todos:
                checkbox = "[x]" if t.get("is_completed") else "[ ]"
                title = t.get("title", "Untitled")
                extras = []
                if t.get("priority"):
                    extras.append("Priority")
                if t.get("is_completed") and t.get("completed_at"):
                    extras.append(f"Completed {_format_date_short(t['completed_at'])}")
                elif t.get("due_at"):
                    extras.append(f"Due: {_format_date_short(t['due_at'])}")
                suffix = f" ({', '.join(extras)})" if extras else ""
                lines.append(f"  {checkbox} {title}{suffix}")

            lines.append("")

        if len(lines) == 3:
            lines.append("No todos found.")

        _atomic_write(os.path.join(WORKSPACE_DIR, "todos.txt"), "\n".join(lines) + "\n")

    # =========================================================================
    # Polling loop
    # =========================================================================

    def _polling_loop(self, interval: int):
        while self._running:
            time.sleep(interval)
            if not self._running:
                break
            try:
                self.full_sync()
            except Exception as e:
                logger.error(f"Polling sync failed: {e}")
'''.lstrip()

REQUIREMENTS_TXT = "anthropic>=0.40.0\nsupabase>=2.0.0\n"

SETUP_SH = r'''
#!/bin/bash
set -e
cd /tmp/runtime
pip install -q -r requirements.txt
nohup python main.py > /tmp/runtime.log 2>&1 &
echo $! > /tmp/runtime.pid
echo "Runtime started (PID: $(cat /tmp/runtime.pid))"
'''.lstrip()


def get_runtime_files() -> Dict[str, str]:
    """Return all runtime files as a filename -> content mapping."""
    return {
        "config.py": CONFIG_PY,
        "step_reporter.py": STEP_REPORTER_PY,
        "tools.py": TOOLS_PY,
        "agent_loop.py": AGENT_LOOP_PY,
        "task_watcher.py": TASK_WATCHER_PY,
        "workspace_sync.py": WORKSPACE_SYNC_PY,
        "main.py": MAIN_PY,
        "requirements.txt": REQUIREMENTS_TXT,
        "setup.sh": SETUP_SH,
    }
