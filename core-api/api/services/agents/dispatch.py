"""
E2B sandbox lifecycle management for AI agents.

Handles creating, resuming, pausing, and destroying E2B sandboxes,
and injecting tasks into running sandboxes for execution.
"""
import io
import json
import logging
import tarfile
import time
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone

from e2b import Sandbox

from api.config import settings
from lib.supabase_client import get_service_role_client
from api.services.agents.runtime_bundle import get_runtime_files

logger = logging.getLogger(__name__)


def _get_e2b_api_key() -> str:
    """Get E2B API key from settings."""
    key = settings.e2b_api_key
    if not key:
        raise ValueError("E2B_API_KEY is not configured")
    return key


def _update_agent_sandbox(agent_id: str, updates: Dict[str, Any]) -> None:
    """Update agent_instances with sandbox info (service role, bypasses RLS)."""
    supabase = get_service_role_client()
    supabase.table("agent_instances").update({
        **updates,
        "last_active_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", agent_id).execute()


def _update_task_status(task_id: str, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """Update agent_tasks status (service role, bypasses RLS)."""
    supabase = get_service_role_client()
    update = {"status": status}
    if status == "running":
        update["started_at"] = datetime.now(timezone.utc).isoformat()
    if extra:
        update.update(extra)
    supabase.table("agent_tasks").update(update).eq("id", task_id).execute()


def get_sandbox_status(sandbox_id: str) -> Optional[str]:
    """
    Check if a sandbox is running, paused, or dead.
    Returns 'running', 'paused', or None (not found / dead).
    """
    try:
        sandbox = Sandbox.connect(
            sandbox_id,
            api_key=_get_e2b_api_key(),
        )
        # If connect succeeds, sandbox is running (or was paused and auto-resumed)
        # Pause it again if we were just checking
        sandbox.beta_pause(api_key=_get_e2b_api_key())
        return "paused"
    except Exception:
        # Could not connect — sandbox doesn't exist or is dead
        return None


def _build_runtime_tarball() -> bytes:
    """Build a tar.gz archive of all runtime files in memory."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for filename, content in get_runtime_files().items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=filename)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _upload_and_start_runtime(sandbox: Sandbox) -> Dict[str, int]:
    """Upload agent runtime files into the sandbox as a tarball and start the process.
    Returns timing dict with phase durations in ms."""
    timings = {}

    t0 = time.perf_counter()
    sandbox.commands.run("mkdir -p /tmp/runtime")
    tarball_data = _build_runtime_tarball()
    sandbox.files.write("/tmp/runtime.tar.gz", tarball_data)
    sandbox.commands.run("cd /tmp/runtime && tar xzf /tmp/runtime.tar.gz")
    timings["upload_and_extract_ms"] = int((time.perf_counter() - t0) * 1000)

    logger.info(f"Runtime tarball uploaded and extracted ({timings['upload_and_extract_ms']}ms), installing dependencies...")

    t0 = time.perf_counter()
    sandbox.commands.run("chmod +x /tmp/runtime/setup.sh")
    sandbox.commands.run("cd /tmp/runtime && bash setup.sh", timeout=120)
    timings["pip_install_and_start_ms"] = int((time.perf_counter() - t0) * 1000)

    logger.info(f"Runtime started inside sandbox ({timings['pip_install_and_start_ms']}ms)")
    return timings


def _check_runtime_alive(sandbox: Sandbox) -> bool:
    """Check if the runtime process is still running in the sandbox."""
    try:
        result = sandbox.commands.run(
            "test -f /tmp/runtime.pid && kill -0 $(cat /tmp/runtime.pid) 2>/dev/null && echo alive || echo dead"
        )
        return "alive" in result.stdout
    except Exception:
        return False


def create_sandbox(agent: Dict[str, Any]) -> Tuple[str, Sandbox]:
    """
    Create a new E2B sandbox for an agent.
    Returns (sandbox_id, sandbox) tuple.
    """
    api_key = _get_e2b_api_key()
    template = settings.e2b_default_template
    boot_start = time.perf_counter()

    logger.info(f"Creating E2B sandbox for agent '{agent['name']}' (template={template})")

    _update_agent_sandbox(agent["id"], {"sandbox_status": "starting"})

    t0 = time.perf_counter()
    sandbox = Sandbox.create(
        template=template,
        timeout=3600,  # 1 hour initial timeout
        metadata={"agent_id": agent["id"], "workspace_id": agent["workspace_id"]},
        envs={
            "AGENT_ID": agent["id"],
            "WORKSPACE_ID": agent["workspace_id"],
            "SUPABASE_URL": settings.supabase_url,
            "SUPABASE_SERVICE_ROLE_KEY": settings.supabase_service_role_key,
            "ANTHROPIC_API_KEY": settings.anthropic_api_key,
            "TIRITH_ENABLED": str(settings.tirith_enabled).lower(),
            "TIRITH_PATH": settings.tirith_path,
            "TIRITH_TIMEOUT": str(settings.tirith_timeout),
            "TIRITH_FAIL_MODE": settings.tirith_fail_mode,
        },
        api_key=api_key,
    )
    e2b_create_ms = int((time.perf_counter() - t0) * 1000)

    sandbox_id = sandbox.sandbox_id
    logger.info(f"E2B sandbox created: {sandbox_id} ({e2b_create_ms}ms)")

    # Create directories the runtime needs
    sandbox.commands.run("mkdir -p /home/user/workspace")

    # Upload and start the agent runtime
    runtime_timings = _upload_and_start_runtime(sandbox)

    total_boot_ms = int((time.perf_counter() - boot_start) * 1000)

    boot_timings = {
        "e2b_create_ms": e2b_create_ms,
        **runtime_timings,
        "total_boot_ms": total_boot_ms,
    }
    logger.info(f"Sandbox boot complete for '{agent['name']}': {json.dumps(boot_timings)}")

    # Update agent record with sandbox ID
    _update_agent_sandbox(agent["id"], {
        "sandbox_id": sandbox_id,
        "sandbox_status": "running",
    })

    return sandbox_id, sandbox


def resume_sandbox(agent: Dict[str, Any]) -> Tuple[str, Sandbox]:
    """
    Resume a paused sandbox. E2B auto-resumes on connect().
    Returns (sandbox_id, sandbox) tuple.
    """
    sandbox_id = agent.get("sandbox_id")
    if not sandbox_id:
        raise ValueError(f"Agent {agent['id']} has no sandbox_id to resume")

    api_key = _get_e2b_api_key()

    logger.info(f"Resuming E2B sandbox {sandbox_id} for agent '{agent['name']}'")

    _update_agent_sandbox(agent["id"], {"sandbox_status": "starting"})

    try:
        sandbox = Sandbox.connect(
            sandbox_id,
            timeout=3600,
            api_key=api_key,
        )
        logger.info(f"E2B sandbox {sandbox_id} resumed")

        # Check if runtime process survived the pause — restart if not
        if not _check_runtime_alive(sandbox):
            logger.info("Runtime process not alive after resume, restarting...")
            _upload_and_start_runtime(sandbox)

        _update_agent_sandbox(agent["id"], {"sandbox_status": "running"})
        return sandbox.sandbox_id, sandbox
    except Exception as e:
        logger.warning(f"Failed to resume sandbox {sandbox_id}: {e}. Creating new one.")
        # Sandbox is dead — create a fresh one
        return create_sandbox(agent)


def ensure_sandbox(agent: Dict[str, Any]) -> Tuple[str, Sandbox]:
    """
    Ensure agent has a running sandbox. Creates or resumes as needed.
    Returns (sandbox_id, sandbox) tuple for connection reuse.
    """
    sandbox_id = agent.get("sandbox_id")
    sandbox_status = agent.get("sandbox_status", "off")

    if not sandbox_id or sandbox_status == "off":
        return create_sandbox(agent)

    if sandbox_status == "paused":
        return resume_sandbox(agent)

    if sandbox_status in ("running", "idle"):
        try:
            api_key = _get_e2b_api_key()
            t0 = time.perf_counter()
            sandbox = Sandbox.connect(sandbox_id, api_key=api_key)
            connect_ms = int((time.perf_counter() - t0) * 1000)
            logger.info(f"Sandbox.connect({sandbox_id}) took {connect_ms}ms")
            return sandbox.sandbox_id, sandbox
        except Exception:
            logger.warning(f"Sandbox {sandbox_id} not reachable, creating new one")
            return create_sandbox(agent)

    logger.warning(f"Agent {agent['id']} has unknown sandbox_status '{sandbox_status}', creating new sandbox")
    return create_sandbox(agent)


def inject_task(sandbox_id: str, task: Dict[str, Any], sandbox: Optional[Sandbox] = None) -> None:
    """
    Write a task file into the sandbox for the runtime to pick up.
    The runtime watches /tmp/tasks/ for new JSON files.
    Accepts an optional sandbox object to reuse an existing connection.
    """
    task_payload = {
        "id": task["id"],
        "agent_id": task["agent_id"],
        "workspace_id": task["workspace_id"],
        "instruction": task["input"].get("instruction", ""),
        "channel_id": task["input"].get("channel_id"),
        "invoked_by": task["input"].get("invoked_by"),
        "conversation_id": task.get("conversation_id"),
        "created_at": task["created_at"],
    }

    if not sandbox:
        api_key = _get_e2b_api_key()
        sandbox = Sandbox.connect(sandbox_id, api_key=api_key)

    sandbox.files.write(
        f"/tmp/tasks/{task['id']}.json",
        json.dumps(task_payload, indent=2),
    )

    logger.info(f"Injected task {task['id']} into sandbox {sandbox_id}")


def pause_sandbox(agent_id: str, sandbox_id: str) -> None:
    """Pause a running sandbox to save costs."""
    api_key = _get_e2b_api_key()

    try:
        sandbox = Sandbox.connect(sandbox_id, api_key=api_key)
        sandbox.beta_pause()
        logger.info(f"Paused sandbox {sandbox_id}")

        _update_agent_sandbox(agent_id, {"sandbox_status": "paused"})
    except Exception as e:
        logger.error(f"Failed to pause sandbox {sandbox_id}: {e}")
        _update_agent_sandbox(agent_id, {"sandbox_status": "error"})


def destroy_sandbox(agent_id: str, sandbox_id: str) -> None:
    """Destroy a sandbox permanently."""
    api_key = _get_e2b_api_key()

    try:
        sandbox = Sandbox.connect(sandbox_id, api_key=api_key)
        sandbox.kill()
        logger.info(f"Destroyed sandbox {sandbox_id}")
    except Exception as e:
        logger.warning(f"Failed to destroy sandbox {sandbox_id} (may already be dead): {e}")

    _update_agent_sandbox(agent_id, {
        "sandbox_id": None,
        "sandbox_status": "off",
    })


def dispatch_task(task_id: str) -> Dict[str, Any]:
    """
    Main dispatch entry point. Called from invoke_agent (background thread).

    For running sandboxes: just update agent status. The runtime polls the DB
    directly for queued tasks, so no Sandbox.connect() or file injection needed.

    For non-running sandboxes: create/resume the sandbox, then the runtime
    will pick up the task from DB once it boots.
    """
    dispatch_start = time.perf_counter()
    supabase = get_service_role_client()

    # Fetch task with joined agent data
    t0 = time.perf_counter()
    task_result = supabase.table("agent_tasks") \
        .select("*, agent:agent_instances(*)") \
        .eq("id", task_id) \
        .single() \
        .execute()

    task = task_result.data
    if not task:
        raise ValueError(f"Task {task_id} not found")

    agent = task.get("agent")
    if not agent:
        raise ValueError(f"Agent not found for task {task_id}")

    fetch_ms = int((time.perf_counter() - t0) * 1000)

    sandbox_id = agent.get("sandbox_id")
    sandbox_status = agent.get("sandbox_status", "off")

    logger.info(
        f"Dispatching task {task_id} for agent '{agent['name']}' "
        f"(fetch={fetch_ms}ms, sandbox={sandbox_status})"
    )

    # Fast path: sandbox is already running — runtime polls DB directly
    # No need for Sandbox.connect() or file injection
    if sandbox_status in ("running", "idle") and sandbox_id:
        t0 = time.perf_counter()
        _update_agent_sandbox(agent["id"], {"status": "working"})
        status_ms = int((time.perf_counter() - t0) * 1000)

        dispatch_ms = int((time.perf_counter() - dispatch_start) * 1000)
        logger.info(
            f"Task {task_id} fast-dispatched (sandbox already running) "
            f"(fetch={fetch_ms}ms, status_update={status_ms}ms, total={dispatch_ms}ms)"
        )
        return {
            "task_id": task_id,
            "sandbox_id": sandbox_id,
            "status": "dispatched",
            "dispatch_ms": dispatch_ms,
        }

    # Slow path: sandbox needs to be created or resumed
    t0 = time.perf_counter()
    _update_agent_sandbox(agent["id"], {"status": "working"})
    status_ms = int((time.perf_counter() - t0) * 1000)

    try:
        t0 = time.perf_counter()
        sandbox_id, sandbox = ensure_sandbox(agent)
        ensure_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(f"ensure_sandbox completed ({ensure_ms}ms)")
    except Exception as e:
        _update_agent_sandbox(agent["id"], {"sandbox_status": "error", "status": "error"})
        _update_task_status(task_id, "failed", extra={"error": f"Sandbox failed: {e}"})
        raise

    _update_agent_sandbox(agent["id"], {"sandbox_status": "running"})

    dispatch_ms = int((time.perf_counter() - dispatch_start) * 1000)
    logger.info(
        f"Task {task_id} dispatched (sandbox created/resumed) "
        f"(fetch={fetch_ms}ms, status={status_ms}ms, ensure={ensure_ms}ms, total={dispatch_ms}ms)"
    )

    return {
        "task_id": task_id,
        "sandbox_id": sandbox_id,
        "status": "dispatched",
        "dispatch_ms": dispatch_ms,
    }
