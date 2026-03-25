# core-api

FastAPI backend for Core — the all-in-one productivity platform.

## Quick Start

```bash
cd core-api
make start
```

**That's it!** The server starts using uv.

API runs at `http://localhost:8000`
- Interactive docs: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/api/health`

## Setup

### First Time Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install runtime + dev dependencies using uv
uv pip install -r requirements-dev.txt

# Create .env file
cp .env.example .env
# Then edit .env with your Supabase credentials
```

### Environment Variables

Create a `.env` file with these variables:

```env
# Core Settings
API_ENV=development
DEBUG=False

# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

# Google OAuth (for Calendar and Gmail sync)
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret

# Google Cloud Pub/Sub (for Gmail push notifications)
# Full topic path: projects/PROJECT_ID/topics/TOPIC_NAME
GOOGLE_PUBSUB_TOPIC=projects/YOUR_PROJECT_ID/topics/gmail-sync-topic
GOOGLE_CLOUD_PROJECT_ID=your_project_id  # Optional fallback

# Webhooks
WEBHOOK_BASE_URL=https://your-api.vercel.app

# Cron Authentication
CRON_SECRET=your_random_secret

# AI Analysis
GROQ_API_KEY=your_groq_api_key

# Workspace invitations (Resend)
RESEND_API_KEY=your_resend_api_key
RESEND_FROM_DOMAIN=yourdomain.com
# Optional explicit sender (overrides RESEND_FROM_DOMAIN)
# RESEND_FROM_EMAIL=Core <invites@yourdomain.com>
# Local dev:
FRONTEND_URL=http://localhost:3000
# Production:
# FRONTEND_URL=https://core.so

# CORS (comma-separated for additional origins)
# ALLOWED_ORIGINS_ENV=https://yourapp.vercel.app
```

## Project Structure

```
core-api/
├── api/
│   ├── config.py         # Settings
│   ├── dependencies.py   # Auth dependencies
│   ├── routers/          # HTTP endpoints (routing only)
│   │   ├── auth.py       # Auth endpoints
│   │   ├── calendar.py   # Calendar endpoints
│   │   ├── tasks.py      # Tasks (Todoist-style, legacy)
│   │   ├── todos.py      # Todos (Reminders-style, new)
│   │   ├── email.py      # Email/Gmail endpoints
│   │   ├── chat.py       # AI chat endpoints (streaming)
│   │   └── ...
│   └── services/         # Business logic (functional code)
│       ├── calendar/     # Calendar operations
│       ├── tasks/        # Task operations (legacy)
│       ├── todos/        # Todo operations (new)
│       │   ├── get_todos.py
│       │   ├── create_todo.py
│       │   ├── update_todo.py
│       │   ├── delete_todo.py
│       │   ├── complete_todo.py
│       │   ├── reorder_todos.py
│       │   ├── habit_helpers.py
│       │   └── calendar_sync.py
│       ├── chat/         # Chat/AI operations
│       │   ├── agent.py          # LLM agent with tools
│       │   ├── content_builder.py # Content parts builder
│       │   ├── events.py         # NDJSON event helpers
│       │   └── tools/            # Tool implementations
│       └── ...
├── lib/
│   └── supabase_client.py # Supabase client
├── supabase/
│   └── migrations/       # Database migrations
├── docs/
│   └── CONTENT_PARTS_SCHEMA.md  # Content parts documentation
├── index.py              # Main FastAPI app (Vercel entry)
├── requirements.txt      # Runtime deps for Vercel
├── requirements-dev.txt  # Local/CI dev deps layered on top
└── vercel.json
```

### Architecture

- **routers/**: HTTP layer - handles requests/responses, validation, HTTP status codes
- **services/**: Business logic layer - all functional operations, database interactions
- **lib/**: Shared utilities and clients

When adding new features:
1. Create service class in `api/services/` with business logic
2. Create router in `api/routers/` that calls the service
3. Register router in `api/index.py`

## Chat & AI

The chat system uses a **Content Parts Schema** for structured message content. This enables:
- Proper interleaving of text and tool outputs during streaming
- Identical rendering during streaming and after reload from database
- Easy extensibility for new tool types

**Documentation:** See [`docs/CONTENT_PARTS_SCHEMA.md`](docs/CONTENT_PARTS_SCHEMA.md) for full details.

### Quick Overview

Messages contain a `content_parts` array with typed parts:

```json
[
  {"type": "text", "data": {"content": "Here are your emails:\n\n"}},
  {"type": "display", "data": {"display_type": "emails", "items": [...], "total_count": 5}},
  {"type": "text", "data": {"content": "\nAnd your todos:\n\n"}},
  {"type": "display", "data": {"display_type": "todos", "items": [...], "total_count": 3}}
]
```

### Adding New Tools

1. **Emit display event** from your tool handler:
   ```python
   yield display_event(display_type="workouts", items=[...], total_count=10)
   ```

2. **Add iOS renderer case** in `ContentPartsRenderer.swift`

3. **Add display content enum case** in `ChatModels.swift`

See the full guide in [`docs/CONTENT_PARTS_SCHEMA.md`](docs/CONTENT_PARTS_SCHEMA.md).

## Available Endpoints

### Core
- `GET /` - Health check
- `GET /api/health` - Detailed health status

### Authentication
- `POST /auth/users` - Create user
- `POST /auth/oauth-connections` - Store OAuth tokens
- `GET /auth/oauth-connections/{user_id}` - Get user connections

### Calendar
- `GET /api/calendar/events` - Get calendar events
- `POST /api/calendar/sync` - Sync from Google Calendar

### Tasks (Legacy - Todoist-style)
- `GET /api/tasks/` - List tasks
- `POST /api/tasks/` - Create task
- `PUT /api/tasks/{id}` - Update task
- `DELETE /api/tasks/{id}` - Delete task

### Todos (New - Reminders-style)
- `GET /api/todos` - List all todos (running list)
- `GET /api/todos/today` - Get todos due today + habits due today
- `GET /api/todos/habits` - List habits only
- `GET /api/todos/{id}` - Get single todo
- `POST /api/todos` - Create todo (201)
- `POST /api/todos/habit` - Create habit (201)
- `PATCH /api/todos/{id}` - Update todo
- `DELETE /api/todos/{id}` - Delete todo
- `PATCH /api/todos/{id}/complete` - Toggle completion (handles habit streaks)
- `POST /api/todos/reorder` - Reorder todos (atomic batch update)

### Chat (AI Assistant)
- `GET /api/chat/conversations` - List conversations
- `POST /api/chat/conversations` - Create conversation
- `PATCH /api/chat/conversations/{id}` - Update conversation title
- `DELETE /api/chat/conversations/{id}` - Delete conversation
- `GET /api/chat/conversations/{id}/messages` - Get messages
- `POST /api/chat/conversations/{id}/messages` - Send message (streaming NDJSON response)

## Database Migrations

Migrations are in `supabase/migrations/`. Run them against your Supabase database:

```bash
# Via Supabase CLI
supabase db push

# Or directly via psql
psql $DATABASE_URL -f supabase/migrations/XXXXXX_migration_name.sql
```

Key tables:
- `todos` - Reminders-style todos with habit support
- `habit_completions` - Tracks habit completion dates for streaks
- `conversations` - Chat conversations
- `messages` - Chat messages with `content` (text) and `content_parts` (structured) columns

RPC functions:
- `calculate_habit_streak(todo_id)` - Efficient streak calculation
- `reorder_todos(positions_json)` - Atomic batch reorder
- `batch_get_habit_streaks(todo_ids[])` - Batch streak lookup

### Content Parts Schema

The `messages.content_parts` column stores structured content:

```sql
-- JSONB column with GIN index
ALTER TABLE messages ADD COLUMN content_parts JSONB;
CREATE INDEX idx_messages_content_parts ON messages USING GIN (content_parts);
```

See [`docs/CONTENT_PARTS_SCHEMA.md`](docs/CONTENT_PARTS_SCHEMA.md) for the full schema.

## Development

```bash
# Run tests
uv run pytest

# Run with auto-reload
uv run python dev.py

# View logs
tail -f logs/api.log
```

## Deployment

Deploy to Vercel:

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel
```

Set environment variables in Vercel dashboard:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_PUBSUB_TOPIC` (for Gmail push notifications)
- `WEBHOOK_BASE_URL` (set to https://your-api.vercel.app for production)
- `CRON_SECRET`
- `GROQ_API_KEY` (for AI email analysis)

## Tech Stack

- **FastAPI** - Modern Python web framework
- **Supabase** - PostgreSQL database + auth
- **Pydantic** - Data validation
- **Mangum** - ASGI adapter for serverless
