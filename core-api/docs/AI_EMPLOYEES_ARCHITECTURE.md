# AI Employees — V1 Implementation Plan

## Goal

Prove the end-to-end agent loop: **message an agent in workspace Messages → it reads your workspace context → does work in an E2B sandbox → replies.**

No agent store, no observability dashboard, no scheduling, no approval workflow. Just the core loop on a branch for testing.

---

## Architecture Overview

```
core-web (React SPA)
  - Add 'agents' mini-app type
  - AgentsView: list agents, see status, quick-invoke
  - Existing MessagesView: DM/mention agents naturally
        │
        │  HTTP + Supabase Realtime
        ▼
core-api (FastAPI on Vercel)
  - New router: /api/agents/
  - New migration: agent_instances, agent_tasks tables
  - Extend channel_messages with nullable agent_id
        │
        │  Supabase table polling (agent_tasks)
        ▼
core-agent (New standalone Python service)
  - Polls agent_tasks for queued work
  - Materializes workspace data as plain text files
  - Spins up E2B sandbox with /workspace and /personal dirs
  - Runs LLM tool-calling loop
  - Posts replies back as channel_messages with agent_id
        │
        │  E2B Python SDK
        ▼
E2B Cloud (Firecracker microVMs)
  - Isolated Linux sandbox per task
  - Chromium, filesystem, shell, code execution
  - /workspace (team context as text files)
  - /personal (agent working directory)
```

---

## Step 1: Database Migration (core-api)

**File:** `supabase/migrations/20260221000001_create_agent_tables.sql`

```sql
-- Migration: Create AI Employees tables (V1)
-- Adds agent_instances and agent_tasks tables, extends channel_messages

-- ============================================================================
-- Table: agent_instances
-- Deployed agents within a workspace
-- ============================================================================

CREATE TABLE public.agent_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES public.workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    avatar_url TEXT,
    status TEXT NOT NULL DEFAULT 'idle',  -- idle, working, error
    system_prompt TEXT NOT NULL,
    enabled_tools TEXT[] NOT NULL DEFAULT '{}',
    config JSONB NOT NULL DEFAULT '{}',   -- model, temperature, max_turns, etc.
    created_by UUID NOT NULL REFERENCES auth.users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_agent_instances_workspace_id ON public.agent_instances(workspace_id);
CREATE INDEX idx_agent_instances_status ON public.agent_instances(status);

ALTER TABLE public.agent_instances ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view agents in their workspaces" ON public.agent_instances
    FOR SELECT USING (
        workspace_id IN (
            SELECT workspace_id FROM public.workspace_members WHERE user_id = auth.uid()
        )
    );

CREATE POLICY "Workspace admins can manage agents" ON public.agent_instances
    FOR ALL USING (
        workspace_id IN (
            SELECT workspace_id FROM public.workspace_members
            WHERE user_id = auth.uid() AND role IN ('owner', 'admin')
        )
    );

CREATE OR REPLACE FUNCTION update_agent_instances_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_agent_instances_updated_at
    BEFORE UPDATE ON public.agent_instances
    FOR EACH ROW
    EXECUTE FUNCTION update_agent_instances_updated_at();

-- ============================================================================
-- Table: agent_tasks
-- Task queue and execution log for agents
-- ============================================================================

CREATE TABLE public.agent_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES public.agent_instances(id) ON DELETE CASCADE,
    workspace_id UUID NOT NULL REFERENCES public.workspaces(id) ON DELETE CASCADE,
    trigger TEXT NOT NULL DEFAULT 'user_message',  -- user_message, api_invoke
    trigger_ref TEXT,                               -- message_id that triggered this
    status TEXT NOT NULL DEFAULT 'queued',           -- queued, running, completed, failed
    input JSONB NOT NULL DEFAULT '{}',              -- instruction + context
    output JSONB,                                    -- final result
    steps JSONB NOT NULL DEFAULT '[]',              -- ordered log of actions taken
    token_usage INTEGER DEFAULT 0,
    error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_agent_tasks_agent_id ON public.agent_tasks(agent_id);
CREATE INDEX idx_agent_tasks_status ON public.agent_tasks(status) WHERE status IN ('queued', 'running');
CREATE INDEX idx_agent_tasks_workspace_id ON public.agent_tasks(workspace_id);
CREATE INDEX idx_agent_tasks_created_at ON public.agent_tasks(created_at DESC);

ALTER TABLE public.agent_tasks ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view tasks in their workspaces" ON public.agent_tasks
    FOR SELECT USING (
        workspace_id IN (
            SELECT workspace_id FROM public.workspace_members WHERE user_id = auth.uid()
        )
    );

CREATE POLICY "Users can create tasks in their workspaces" ON public.agent_tasks
    FOR INSERT WITH CHECK (
        workspace_id IN (
            SELECT workspace_id FROM public.workspace_members WHERE user_id = auth.uid()
        )
    );

-- ============================================================================
-- Extend channel_messages: add nullable agent_id
-- Messages have either user_id OR agent_id (agent_id = null for human messages)
-- ============================================================================

ALTER TABLE public.channel_messages
    ADD COLUMN IF NOT EXISTS agent_id UUID REFERENCES public.agent_instances(id) ON DELETE SET NULL;

CREATE INDEX idx_channel_messages_agent_id ON public.channel_messages(agent_id)
    WHERE agent_id IS NOT NULL;

-- Make user_id nullable (agent messages won't have a user_id)
ALTER TABLE public.channel_messages
    ALTER COLUMN user_id DROP NOT NULL;

-- ============================================================================
-- Add 'agents' to mini_app_type enum
-- ============================================================================

ALTER TYPE mini_app_type ADD VALUE IF NOT EXISTS 'agents';

-- ============================================================================
-- Enable Realtime for agent_tasks (core-agent polls, but web gets live updates)
-- ============================================================================

ALTER PUBLICATION supabase_realtime ADD TABLE public.agent_tasks;
ALTER PUBLICATION supabase_realtime ADD TABLE public.agent_instances;
```

---

## Step 2: Agent Router (core-api)

**File:** `api/routers/agents.py`

```python
"""
Agent management and invocation endpoints.
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import logging

from api.dependencies import get_current_user_id, get_current_user_jwt
from api.services.agents import agents as agent_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["agents"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class CreateAgentRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    workspace_id: str
    system_prompt: str = Field(..., min_length=1)
    enabled_tools: List[str] = []
    config: Dict[str, Any] = {}
    avatar_url: Optional[str] = None

class InvokeAgentRequest(BaseModel):
    instruction: str = Field(..., min_length=1)
    channel_id: Optional[str] = None  # reply in this channel

class AgentResponse(BaseModel):
    id: str
    workspace_id: str
    name: str
    avatar_url: Optional[str] = None
    status: str
    system_prompt: str
    enabled_tools: List[str]
    config: Dict[str, Any]
    created_by: str
    created_at: str
    updated_at: str

    class Config:
        extra = "allow"

class AgentListResponse(BaseModel):
    agents: List[AgentResponse]
    count: int

class TaskResponse(BaseModel):
    id: str
    agent_id: str
    status: str
    input: Dict[str, Any]
    output: Optional[Dict[str, Any]] = None
    created_at: str

    class Config:
        extra = "allow"


# =============================================================================
# AGENT CRUD ENDPOINTS
# =============================================================================

@router.get("/workspaces/{workspace_id}/agents", response_model=AgentListResponse)
async def list_agents(
    workspace_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List all agents in a workspace."""
    try:
        agents = await agent_service.get_agents(workspace_id, user_jwt)
        return {"agents": agents, "count": len(agents)}
    except Exception as e:
        logger.error(f"Error listing agents: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workspaces/{workspace_id}/agents", response_model=AgentResponse)
async def create_agent(
    workspace_id: str,
    request: CreateAgentRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Create a new agent in a workspace."""
    try:
        agent = await agent_service.create_agent(
            workspace_id=workspace_id,
            name=request.name,
            system_prompt=request.system_prompt,
            enabled_tools=request.enabled_tools,
            config=request.config,
            avatar_url=request.avatar_url,
            created_by=user_id,
            user_jwt=user_jwt,
        )
        return agent
    except Exception as e:
        logger.error(f"Error creating agent: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/agents/{agent_id}/invoke", response_model=TaskResponse)
async def invoke_agent(
    agent_id: str,
    request: InvokeAgentRequest,
    user_id: str = Depends(get_current_user_id),
    user_jwt: str = Depends(get_current_user_jwt),
):
    """Queue a task for an agent to execute."""
    try:
        task = await agent_service.invoke_agent(
            agent_id=agent_id,
            instruction=request.instruction,
            channel_id=request.channel_id,
            user_id=user_id,
            user_jwt=user_jwt,
        )
        return task
    except Exception as e:
        logger.error(f"Error invoking agent: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/agents/{agent_id}/tasks", response_model=List[TaskResponse])
async def list_agent_tasks(
    agent_id: str,
    user_jwt: str = Depends(get_current_user_jwt),
):
    """List recent tasks for an agent."""
    try:
        tasks = await agent_service.get_agent_tasks(agent_id, user_jwt)
        return tasks
    except Exception as e:
        logger.error(f"Error listing agent tasks: {e}")
        raise HTTPException(status_code=400, detail=str(e))
```

**File:** `api/services/agents/__init__.py` — empty

**File:** `api/services/agents/agents.py`

```python
"""
Agent management service.
"""
from typing import List, Dict, Any, Optional
import logging

from lib.supabase_client import get_authenticated_async_client

logger = logging.getLogger(__name__)


async def get_agents(workspace_id: str, user_jwt: str) -> List[Dict[str, Any]]:
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        result = await (
            supabase.table("agent_instances")
            .select("*")
            .eq("workspace_id", workspace_id)
            .order("created_at")
            .execute()
        )
        agents = result.data or []
        logger.info(f"Retrieved {len(agents)} agents for workspace {workspace_id}")
        return agents
    except Exception as e:
        logger.error(f"Error getting agents: {e}")
        raise


async def create_agent(
    workspace_id: str,
    name: str,
    system_prompt: str,
    enabled_tools: List[str],
    config: Dict[str, Any],
    avatar_url: Optional[str],
    created_by: str,
    user_jwt: str,
) -> Dict[str, Any]:
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        result = await (
            supabase.table("agent_instances")
            .insert({
                "workspace_id": workspace_id,
                "name": name,
                "system_prompt": system_prompt,
                "enabled_tools": enabled_tools,
                "config": config,
                "avatar_url": avatar_url,
                "created_by": created_by,
            })
            .execute()
        )
        agent = result.data[0]
        logger.info(f"Created agent '{name}' ({agent['id']}) in workspace {workspace_id}")
        return agent
    except Exception as e:
        logger.error(f"Error creating agent: {e}")
        raise


async def invoke_agent(
    agent_id: str,
    instruction: str,
    channel_id: Optional[str],
    user_id: str,
    user_jwt: str,
) -> Dict[str, Any]:
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        # Get agent to find workspace_id
        agent_result = await (
            supabase.table("agent_instances")
            .select("id, workspace_id")
            .eq("id", agent_id)
            .single()
            .execute()
        )
        agent = agent_result.data

        # Create task
        result = await (
            supabase.table("agent_tasks")
            .insert({
                "agent_id": agent_id,
                "workspace_id": agent["workspace_id"],
                "trigger": "user_message",
                "input": {
                    "instruction": instruction,
                    "channel_id": channel_id,
                    "invoked_by": user_id,
                },
            })
            .execute()
        )
        task = result.data[0]
        logger.info(f"Queued task {task['id']} for agent {agent_id}")
        return task
    except Exception as e:
        logger.error(f"Error invoking agent: {e}")
        raise


async def get_agent_tasks(agent_id: str, user_jwt: str) -> List[Dict[str, Any]]:
    supabase = await get_authenticated_async_client(user_jwt)
    try:
        result = await (
            supabase.table("agent_tasks")
            .select("*")
            .eq("agent_id", agent_id)
            .order("created_at", desc=True)
            .limit(20)
            .execute()
        )
        return result.data or []
    except Exception as e:
        logger.error(f"Error getting agent tasks: {e}")
        raise
```

**Register router in `index.py`:**

Add to imports:
```python
from api.routers import agents
```

Add to router registration:
```python
app.include_router(agents.router)
```

---

## Step 3: core-agent Service (new standalone project)

**Directory:** `core-agent/v2/`

### 3a. Project Setup

**File:** `core-agent/v2/pyproject.toml`
```toml
[project]
name = "core-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "supabase>=2.0.0",
    "e2b>=1.0.0",
    "e2b-desktop>=1.0.0",
    "openai>=1.0.0",
    "httpx>=0.27.0",
    "python-dotenv>=1.0.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
]
```

**File:** `core-agent/v2/.env` (extend existing)
```
OPENAI_API_KEY=sk-proj-...
E2B_API_KEY=e2b_...
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJ...
```

### 3b. Entry Point

**File:** `core-agent/v2/main.py`
```python
"""
Core Agent — polls agent_tasks and runs agent loops in E2B sandboxes.
"""
import asyncio
import logging
from dotenv import load_dotenv

from config import settings
from runtime.task_runner import TaskRunner

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main():
    logger.info("core-agent starting...")
    runner = TaskRunner()
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### 3c. Config

**File:** `core-agent/v2/config.py`
```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_role_key: str
    openai_api_key: str
    e2b_api_key: str

    poll_interval_seconds: int = 2
    max_agent_turns: int = 20
    default_model: str = "gpt-4o"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
```

### 3d. Task Runner (polls for work)

**File:** `core-agent/v2/runtime/task_runner.py`
```python
"""
Polls agent_tasks table for queued tasks and dispatches them.
"""
import asyncio
import logging
from supabase import create_client

from config import settings
from runtime.agent_loop import run_agent_task

logger = logging.getLogger(__name__)


class TaskRunner:
    def __init__(self):
        self.supabase = create_client(settings.supabase_url, settings.supabase_service_role_key)

    async def run(self):
        logger.info(f"Polling agent_tasks every {settings.poll_interval_seconds}s...")
        while True:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"Poll error: {e}")
            await asyncio.sleep(settings.poll_interval_seconds)

    async def _poll_once(self):
        result = (
            self.supabase.table("agent_tasks")
            .select("*, agent:agent_instances(*)")
            .eq("status", "queued")
            .order("created_at")
            .limit(1)
            .execute()
        )

        if not result.data:
            return

        task = result.data[0]
        agent = task["agent"]
        logger.info(f"Picked up task {task['id']} for agent '{agent['name']}'")

        # Mark running
        self.supabase.table("agent_tasks").update({
            "status": "running",
            "started_at": "now()",
        }).eq("id", task["id"]).execute()

        # Update agent status
        self.supabase.table("agent_instances").update({
            "status": "working",
        }).eq("id", agent["id"]).execute()

        try:
            output = await run_agent_task(self.supabase, agent, task)
            self.supabase.table("agent_tasks").update({
                "status": "completed",
                "output": output,
                "completed_at": "now()",
            }).eq("id", task["id"]).execute()
            logger.info(f"Task {task['id']} completed")
        except Exception as e:
            logger.error(f"Task {task['id']} failed: {e}")
            self.supabase.table("agent_tasks").update({
                "status": "failed",
                "error": str(e),
                "completed_at": "now()",
            }).eq("id", task["id"]).execute()
        finally:
            self.supabase.table("agent_instances").update({
                "status": "idle",
            }).eq("id", agent["id"]).execute()
```

### 3e. Agent Loop (LLM reasoning + E2B execution)

**File:** `core-agent/v2/runtime/agent_loop.py`
```python
"""
Core agent loop: materialize context → spin up E2B → LLM tool loop → post reply.
"""
import json
import logging
from typing import Dict, Any
from openai import AsyncOpenAI

from config import settings
from runtime.materializer import materialize_workspace
from sandbox.manager import SandboxManager
from tools.definitions import get_tool_definitions, execute_tool

logger = logging.getLogger(__name__)

client = AsyncOpenAI(api_key=settings.openai_api_key)


async def run_agent_task(supabase, agent: Dict, task: Dict) -> Dict[str, Any]:
    workspace_id = agent["workspace_id"]
    instruction = task["input"]["instruction"]
    channel_id = task["input"].get("channel_id")
    steps = []

    # 1. Materialize workspace context as plain text
    logger.info(f"Materializing workspace {workspace_id}...")
    workspace_text = await materialize_workspace(supabase, workspace_id)

    # 2. Spin up E2B sandbox
    logger.info("Creating E2B sandbox...")
    sandbox_mgr = SandboxManager()
    sandbox = await sandbox_mgr.create()

    # Write workspace context into sandbox filesystem
    await sandbox_mgr.write_file("/workspace/context.md", workspace_text)
    await sandbox_mgr.ensure_dirs(["/personal/memory", "/personal/drafts"])

    try:
        # 3. Build conversation
        messages = [
            {
                "role": "system",
                "content": (
                    f"{agent['system_prompt']}\n\n"
                    f"## Your Workspace\n\n"
                    f"Your workspace files are at /workspace/ in your sandbox.\n"
                    f"Your personal working directory is /personal/.\n\n"
                    f"Here is the current state of your team's workspace:\n\n"
                    f"{workspace_text}"
                ),
            },
            {"role": "user", "content": instruction},
        ]

        tools = get_tool_definitions(agent.get("enabled_tools", []))

        # 4. LLM tool-calling loop
        final_response = None
        for turn in range(settings.max_agent_turns):
            response = await client.chat.completions.create(
                model=agent.get("config", {}).get("model", settings.default_model),
                messages=messages,
                tools=tools if tools else None,
            )

            choice = response.choices[0]
            messages.append(choice.message)

            steps.append({
                "turn": turn,
                "finish_reason": choice.finish_reason,
                "tool_calls": [
                    {"name": tc.function.name, "args": tc.function.arguments}
                    for tc in (choice.message.tool_calls or [])
                ],
            })

            # Done — agent produced final text response
            if choice.finish_reason == "stop":
                final_response = choice.message.content
                break

            # Tool calls — execute and feed back
            if choice.message.tool_calls:
                for tool_call in choice.message.tool_calls:
                    result = await execute_tool(
                        tool_call.function.name,
                        json.loads(tool_call.function.arguments),
                        sandbox_mgr,
                        supabase,
                        agent,
                    )
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(result),
                    })

        # 5. Post reply to channel
        if final_response and channel_id:
            await post_agent_message(supabase, agent, channel_id, final_response)

        # Update task with step log
        supabase.table("agent_tasks").update({
            "steps": steps,
            "token_usage": sum(
                getattr(r, "usage", None) and r.usage.total_tokens or 0
                for r in [response]
            ),
        }).eq("id", task["id"]).execute()

        return {"response": final_response, "turns": len(steps)}

    finally:
        await sandbox_mgr.close()


async def post_agent_message(supabase, agent: Dict, channel_id: str, content: str):
    """Post a message as the agent to a channel."""
    supabase.table("channel_messages").insert({
        "channel_id": channel_id,
        "agent_id": agent["id"],
        "user_id": None,
        "content": content,
        "blocks": [{"type": "text", "data": {"text": content}}],
    }).execute()
    logger.info(f"Agent '{agent['name']}' posted to channel {channel_id}")
```

### 3f. Workspace Materializer

**File:** `core-agent/v2/runtime/materializer.py`
```python
"""
Renders workspace data from Supabase into plain text files for agent context.
"""
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


async def materialize_workspace(supabase, workspace_id: str) -> str:
    """Build a plain-text snapshot of everything in the workspace."""
    sections = []

    # Get workspace apps to find relevant app IDs
    apps_result = supabase.table("workspace_apps") \
        .select("id, app_type") \
        .eq("workspace_id", workspace_id) \
        .execute()
    apps = {app["app_type"]: app["id"] for app in (apps_result.data or [])}

    # Messages
    if "messages" in apps:
        messages_section = await _materialize_messages(supabase, apps["messages"])
        if messages_section:
            sections.append(messages_section)

    # Files
    if "files" in apps:
        files_section = await _materialize_files(supabase, apps["files"])
        if files_section:
            sections.append(files_section)

    # Projects
    if "projects" in apps:
        projects_section = await _materialize_projects(supabase, apps["projects"])
        if projects_section:
            sections.append(projects_section)

    # Todos/Tasks
    if "tasks" in apps:
        tasks_section = await _materialize_todos(supabase, apps["tasks"])
        if tasks_section:
            sections.append(tasks_section)

    return "\n\n---\n\n".join(sections) if sections else "(Empty workspace)"


async def _materialize_messages(supabase, app_id: str) -> str:
    """Render recent messages from all channels."""
    channels_result = supabase.table("channels") \
        .select("id, name") \
        .eq("workspace_app_id", app_id) \
        .eq("is_dm", False) \
        .execute()

    lines = ["# Messages\n"]
    for channel in (channels_result.data or []):
        msgs_result = supabase.table("channel_messages") \
            .select("content, created_at, user:users(name)") \
            .eq("channel_id", channel["id"]) \
            .order("created_at", desc=True) \
            .limit(50) \
            .execute()

        messages = list(reversed(msgs_result.data or []))
        if not messages:
            continue

        lines.append(f"\n## #{channel['name']}\n")
        for msg in messages:
            time = _format_time(msg["created_at"])
            author = (msg.get("user") or {}).get("name", "Unknown")
            lines.append(f"[{time}] {author}: {msg['content']}")

    return "\n".join(lines) if len(lines) > 1 else ""


async def _materialize_files(supabase, app_id: str) -> str:
    """Render file tree."""
    docs_result = supabase.table("documents") \
        .select("title, type, is_folder, parent_id") \
        .eq("workspace_app_id", app_id) \
        .eq("is_archived", False) \
        .order("position") \
        .execute()

    if not docs_result.data:
        return ""

    lines = ["# Files\n"]
    # Simple flat list for V1
    for doc in docs_result.data:
        prefix = "📁" if doc.get("is_folder") else "  "
        lines.append(f"{prefix} {doc['title']}")

    return "\n".join(lines)


async def _materialize_projects(supabase, app_id: str) -> str:
    """Render project boards."""
    boards_result = supabase.table("project_boards") \
        .select("id, name") \
        .eq("workspace_app_id", app_id) \
        .execute()

    if not boards_result.data:
        return ""

    lines = ["# Projects\n"]
    for board in boards_result.data:
        lines.append(f"\n## {board['name']}\n")

        states_result = supabase.table("project_states") \
            .select("id, name") \
            .eq("board_id", board["id"]) \
            .order("position") \
            .execute()

        for state in (states_result.data or []):
            lines.append(f"\n### {state['name']}")
            issues_result = supabase.table("project_issues") \
                .select("number, title, priority") \
                .eq("state_id", state["id"]) \
                .order("position") \
                .execute()
            for issue in (issues_result.data or []):
                lines.append(f"- [{board['name'][:3].upper()}-{issue['number']}] {issue['title']}")

    return "\n".join(lines)


async def _materialize_todos(supabase, app_id: str) -> str:
    """Render todo list."""
    todos_result = supabase.table("todos") \
        .select("title, is_completed, due_at, priority") \
        .eq("workspace_app_id", app_id) \
        .order("created_at", desc=True) \
        .limit(50) \
        .execute()

    if not todos_result.data:
        return ""

    lines = ["# Tasks\n"]
    for todo in todos_result.data:
        check = "x" if todo.get("is_completed") else " "
        due = f" (due {todo['due_at'][:10]})" if todo.get("due_at") else ""
        lines.append(f"- [{check}] {todo['title']}{due}")

    return "\n".join(lines)


def _format_time(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %I:%M%p").replace(" 0", " ")
    except Exception:
        return iso_str
```

### 3g. E2B Sandbox Manager

**File:** `core-agent/v2/sandbox/manager.py`
```python
"""
Manages E2B sandbox lifecycle.
"""
import logging
from typing import List
from e2b import Sandbox

from config import settings

logger = logging.getLogger(__name__)


class SandboxManager:
    def __init__(self):
        self.sandbox: Sandbox = None

    async def create(self, template: str = "base") -> Sandbox:
        self.sandbox = Sandbox(api_key=settings.e2b_api_key)
        logger.info(f"E2B sandbox created: {self.sandbox.sandbox_id}")
        return self.sandbox

    async def write_file(self, path: str, content: str):
        self.sandbox.files.write(path, content)

    async def read_file(self, path: str) -> str:
        return self.sandbox.files.read(path)

    async def ensure_dirs(self, paths: List[str]):
        for path in paths:
            self.sandbox.commands.run(f"mkdir -p {path}")

    async def run_command(self, cmd: str) -> str:
        result = self.sandbox.commands.run(cmd)
        return result.stdout + result.stderr

    async def close(self):
        if self.sandbox:
            self.sandbox.kill()
            logger.info(f"E2B sandbox {self.sandbox.sandbox_id} destroyed")
```

### 3h. Tool Definitions

**File:** `core-agent/v2/tools/definitions.py`
```python
"""
V1 tool definitions for agent sandbox interaction.
"""
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# V1 tools — keep it minimal
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_shell",
            "description": "Run a shell command in the sandbox",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a file from the sandbox filesystem",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute file path"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file in /personal/ directory",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path (must be under /personal/)"},
                    "content": {"type": "string", "description": "File content"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_message",
            "description": "Send a message to a workspace channel",
            "parameters": {
                "type": "object",
                "properties": {
                    "channel_id": {"type": "string", "description": "Channel ID to post in"},
                    "content": {"type": "string", "description": "Message text"},
                },
                "required": ["channel_id", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "browse_url",
            "description": "Navigate to a URL in the sandbox browser and return the page text",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL to navigate to"},
                },
                "required": ["url"],
            },
        },
    },
]


def get_tool_definitions(enabled_tools: List[str]) -> List[Dict]:
    """Get tool definitions. V1: return all tools regardless of enabled_tools filter."""
    return TOOLS


async def execute_tool(
    name: str,
    args: Dict[str, Any],
    sandbox_mgr,
    supabase,
    agent: Dict,
) -> Dict[str, Any]:
    """Execute a tool and return the result."""
    try:
        if name == "run_shell":
            output = await sandbox_mgr.run_command(args["command"])
            return {"status": "ok", "output": output[:5000]}

        elif name == "read_file":
            content = await sandbox_mgr.read_file(args["path"])
            return {"status": "ok", "content": content[:10000]}

        elif name == "write_file":
            path = args["path"]
            if not path.startswith("/personal/"):
                return {"status": "error", "message": "Can only write to /personal/"}
            await sandbox_mgr.write_file(path, args["content"])
            return {"status": "ok", "message": f"Written to {path}"}

        elif name == "send_message":
            supabase.table("channel_messages").insert({
                "channel_id": args["channel_id"],
                "agent_id": agent["id"],
                "user_id": None,
                "content": args["content"],
                "blocks": [{"type": "text", "data": {"text": args["content"]}}],
            }).execute()
            return {"status": "ok", "message": "Message sent"}

        elif name == "browse_url":
            output = await sandbox_mgr.run_command(
                f"curl -s -L --max-time 10 '{args['url']}' | head -c 20000"
            )
            return {"status": "ok", "content": output[:10000]}

        else:
            return {"status": "error", "message": f"Unknown tool: {name}"}

    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        return {"status": "error", "message": str(e)}
```

---

## Step 4: Frontend — Minimal (core-web)

### 4a. Add 'agents' to MiniAppType

**File:** `src/types/index.ts`

Add `'agents'` to the union and `AVAILABLE_MINI_APPS`:

```typescript
export type MiniAppType = 'tasks' | 'files' | 'biometrics' | 'exercise' | 'sounds' | 'dashboard' | 'projects' | 'agents';

export const AVAILABLE_MINI_APPS: { type: MiniAppType; name: string; icon: string }[] = [
  // ... existing entries ...
  { type: 'agents', name: 'Agents', icon: 'Robot' },
];
```

### 4b. Add ChannelMessage.agent_id

**File:** `src/api/client.ts`

Extend the `ChannelMessage` interface:

```typescript
export interface ChannelMessage {
  // ... existing fields ...
  agent_id?: string;        // set when message is from an agent
  agent?: AgentInstance;     // joined agent data
}

export interface AgentInstance {
  id: string;
  workspace_id: string;
  name: string;
  avatar_url?: string;
  status: 'idle' | 'working' | 'error';
  system_prompt: string;
  enabled_tools: string[];
  config: Record<string, unknown>;
  created_by: string;
  created_at: string;
  updated_at: string;
}
```

Add API functions:

```typescript
export async function getWorkspaceAgents(workspaceId: string): Promise<{ agents: AgentInstance[]; count: number }> {
  return api(`/workspaces/${workspaceId}/agents`);
}

export async function createAgent(workspaceId: string, data: {
  name: string;
  system_prompt: string;
  enabled_tools?: string[];
  config?: Record<string, unknown>;
}): Promise<AgentInstance> {
  return api(`/workspaces/${workspaceId}/agents`, {
    method: 'POST',
    body: JSON.stringify({ ...data, workspace_id: workspaceId }),
  });
}

export async function invokeAgent(agentId: string, instruction: string, channelId?: string): Promise<{ id: string; status: string }> {
  return api(`/agents/${agentId}/invoke`, {
    method: 'POST',
    body: JSON.stringify({ instruction, channel_id: channelId }),
  });
}
```

### 4c. AgentsView Component (basic)

**File:** `src/components/Agents/AgentsView.tsx`

Simple list of agents with status indicators and an invoke button. Displays agents in the workspace, allows creating a new one (hardcoded research agent template for V1), and shows recent tasks.

### 4d. Route Registration

**File:** `src/App.tsx`

Add route:
```tsx
<Route path="/workspace/:workspaceId/agents" element={<AgentsView />} />
```

### 4e. Message Rendering — Show Agent Messages

**File:** `src/components/Messages/MessageItem.tsx` (or equivalent)

Small change: when rendering a message, check `message.agent_id`. If set, show agent name/avatar instead of user. Add a small "Agent" badge.

---

## Step 5: Seed Test Agent

After migration, manually seed one agent via Supabase SQL editor or the new API:

```sql
INSERT INTO public.agent_instances (workspace_id, name, system_prompt, enabled_tools, created_by)
VALUES (
    '<your-workspace-id>',
    'Research Agent',
    'You are a research agent working for your team. You have access to a sandbox with a browser and filesystem. Read your workspace context at /workspace/context.md to understand what your team is working on. Use your tools to research topics and provide helpful information. Always be concise and actionable.',
    ARRAY['run_shell', 'read_file', 'write_file', 'send_message', 'browse_url'],
    '<your-user-id>'
);
```

---

## File Inventory (what we're creating)

### core-api (existing repo, new branch)
| File | Action | Lines (est) |
|---|---|---|
| `supabase/migrations/20260221000001_create_agent_tables.sql` | Create | ~80 |
| `api/routers/agents.py` | Create | ~120 |
| `api/services/agents/__init__.py` | Create | 0 |
| `api/services/agents/agents.py` | Create | ~100 |
| `index.py` | Edit (2 lines: import + include_router) | +2 |

### core-agent (new service, in existing dir)
| File | Action | Lines (est) |
|---|---|---|
| `v2/pyproject.toml` | Create | ~15 |
| `v2/.env` | Edit (add 3 keys) | +3 |
| `v2/main.py` | Create | ~20 |
| `v2/config.py` | Create | ~20 |
| `v2/runtime/__init__.py` | Create | 0 |
| `v2/runtime/task_runner.py` | Create | ~70 |
| `v2/runtime/agent_loop.py` | Create | ~120 |
| `v2/runtime/materializer.py` | Create | ~140 |
| `v2/sandbox/__init__.py` | Create | 0 |
| `v2/sandbox/manager.py` | Create | ~40 |
| `v2/tools/__init__.py` | Create | 0 |
| `v2/tools/definitions.py` | Create | ~130 |

### core-web (existing repo, new branch)
| File | Action | Lines (est) |
|---|---|---|
| `src/types/index.ts` | Edit (add 'agents' type) | +2 |
| `src/api/client.ts` | Edit (add AgentInstance type + 3 functions) | +30 |
| `src/components/Agents/AgentsView.tsx` | Create | ~150 |
| `src/App.tsx` | Edit (add route) | +2 |
| Message rendering component | Edit (agent badge) | +10 |

**Total: ~1,050 lines of new code across 3 projects. 3 edits to existing files (all additive).**

---

## Testing Flow

1. Run the migration against Supabase
2. Seed a test agent via SQL
3. Start core-agent locally: `cd core-agent/v2 && python main.py`
4. Open core-web, go to workspace Messages
5. Create a DM channel with the agent (or invoke via API: `POST /api/agents/{id}/invoke`)
6. Watch core-agent logs pick up the task
7. Agent materializes workspace, spins up E2B, runs LLM loop
8. Agent reply appears in the channel via Supabase Realtime

---

## What's Deliberately Left Out of V1

- Agent Store / templates UI
- Observability dashboard
- Scheduling / cron
- Approval workflow
- Real-time sync to E2B (materialized once at task start)
- Agent memory persistence
- Multi-model support (hardcoded to GPT-4o)
- Agent-to-agent communication
- Budget limits / cost tracking
