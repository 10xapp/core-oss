# Core

Open-source, all-in-one productivity platform. Email, calendar, chat (AI), messages, files, projects, and dashboard — in a single app.

**Core** is a monorepo with three packages:

- **[core-api](./core-api)** — Python/FastAPI backend
- **[core-web](./core-web)** — React/Vite frontend
- **[core-image-proxy](./core-image-proxy)** — Cloudflare Worker for HMAC-signed image resizing and CDN caching

## Features

- **Email** — Gmail and Outlook sync with AI-powered summaries and importance detection
- **Calendar** — Google Calendar and Microsoft 365 sync with event management
- **Chat** — AI assistant with tool use (email search, web search, and more)
- **Messages** — Real-time team messaging with channels, threads, and reactions
- **Files** — Upload, preview, and organize files with presigned URL uploads
- **Documents** — Rich text editor (TipTap/ProseMirror) with real-time collaboration
- **Projects** — Kanban boards with issues, labels, and assignees
- **Workspaces** — Multi-workspace support with role-based access control and resource sharing
- **Dashboard** — Unified view across all apps

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.12, FastAPI, Pydantic |
| Frontend | React 19, Vite 7, TypeScript, Tailwind 4 |
| Database | Supabase (PostgreSQL + RLS + Realtime) |
| Auth | Supabase Auth (JWT), Google/Microsoft OAuth |
| State | Zustand with persistence |
| Rich Text | TipTap (ProseMirror) |
| File Storage | Cloudflare R2 (S3-compatible) |
| Deployment | Vercel (API + Web) |

## Quick Start

### Prerequisites

- [Node.js](https://nodejs.org/) >= 18
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- A [Supabase](https://supabase.com) project (free tier works)

### 1. Clone and configure

```bash
git clone https://github.com/10xapp/core-oss.git
cd core-oss

# Set up environment variables
cp core-api/.env.example core-api/.env
cp core-web/.env.example core-web/.env

# Edit both .env files with your Supabase credentials
```

### 2. Set up the database

```bash
# Install Supabase CLI: https://supabase.com/docs/guides/cli
cd core-api
supabase link --project-ref YOUR_PROJECT_REF
supabase db push
```

### 3. Start the backend

```bash
cd core-api
uv pip install -r requirements.txt
make start
# API runs at http://localhost:8000
```

### 4. Start the frontend

```bash
cd core-web
npm install
npm run dev
# App runs at http://localhost:5173
```

## Architecture

```
core-oss/
├── core-api/                # FastAPI backend
│   ├── api/
│   │   ├── config.py        # Pydantic settings (env-driven)
│   │   ├── dependencies.py  # Auth dependencies
│   │   ├── routers/         # HTTP endpoints
│   │   └── services/        # Business logic
│   ├── lib/                 # Shared clients (Supabase, R2, etc.)
│   ├── supabase/migrations/ # 23 domain-organized SQL migrations
│   └── tests/
├── core-web/                # React SPA
│   ├── src/
│   │   ├── api/             # API client
│   │   ├── components/      # 34 feature modules
│   │   ├── stores/          # Zustand stores
│   │   ├── hooks/           # Custom hooks
│   │   └── lib/             # Supabase, Sentry, PostHog clients
│   └── public/
├── core-image-proxy/        # Cloudflare Worker
│   └── src/index.ts         # HMAC-signed image proxy + resizing
├── LICENSE                  # Apache 2.0
└── README.md
```

## Service Dependencies

Core is designed to run with **just Supabase** for local development. Additional services unlock more features, and most can be swapped for self-hosted or alternative providers.

| Service | Required | What it enables | Without it | Self-hosted / Alternatives |
|---------|----------|----------------|------------|---------------------------|
| **Supabase** | Yes | Auth, database, realtime | — | [Self-hosted Supabase](https://supabase.com/docs/guides/self-hosting/docker) (Docker) |
| **Cloudflare R2** | No | File uploads, avatars | File features disabled | Any S3-compatible store: [Garage](https://garagehq.deuxfleurs.fr/), MinIO, AWS S3, DigitalOcean Spaces — uses boto3, just change env vars |
| **OpenAI / Anthropic** | No | AI chat assistant | Chat disabled | [vLLM](https://github.com/vllm-project/vllm) + Qwen3/Llama 4 (best for tool calling), [Ollama](https://ollama.com), or [OpenRouter](https://openrouter.ai) for multi-provider access |
| **Groq** | No | Email AI analysis | Generic email summaries | [Ollama](https://ollama.com) + Qwen3-8B, or any OpenAI-compatible API |
| **Google OAuth** | No | Gmail + Google Calendar sync | Email/calendar sync disabled | Requires Google Cloud credentials — no alternative |
| **Microsoft OAuth** | No | Outlook + M365 Calendar sync | Microsoft sync disabled | Requires Azure AD credentials — no alternative |
| **Resend** | No | Workspace invitation emails | Invitations fail | Any SMTP provider: Gmail SMTP, Mailgun, Amazon SES — or [Mailpit](https://github.com/axllent/mailpit) for local dev |
| **Redis** | No | Distributed rate limiting | In-memory fallback | [Valkey](https://github.com/valkey-io/valkey) (BSD-licensed Redis fork), Dragonfly, or plain Redis |
| **QStash** | No | Background job queue | Jobs handled by cron | No swap needed — cron endpoints handle sync without it |
| **Exa** | No | Web search in AI chat | Web search tool disabled | [SearXNG](https://github.com/searxng/searxng) (free, self-hosted), [Tavily](https://tavily.com) (1k free/mo), Brave Search API |
| **Sentry** | No | Error tracking | No error reporting | [Bugsink](https://www.bugsink.com) (single container, Sentry SDK compatible), [GlitchTip](https://glitchtip.com), or self-hosted Sentry |
| **PostHog** | No | Analytics | No analytics | [Self-hosted PostHog](https://posthog.com/docs/self-host) (Docker) |

> **Note:** The AI chat agent uses tool calling (function calling) with 8+ tools. If self-hosting an LLM, use vLLM — Ollama's OpenAI-compatible endpoint drops tool calls during streaming. [LiteLLM](https://github.com/BerriAI/litellm) can act as a proxy to normalize any LLM backend into an OpenAI-compatible API.

See [`core-api/.env.example`](./core-api/.env.example) for the full list with setup instructions.

## Development

### Running checks locally

```bash
# Backend — lint + type check
cd core-api
make check        # runs both lint and typecheck
make test         # run tests
make lint         # ruff linter only
make typecheck    # mypy only

# Frontend — build + lint
cd core-web
npm run build     # TypeScript check + Vite build
npm run lint      # ESLint
```

### Pre-commit hooks (secret scanning)

We use [gitleaks](https://github.com/gitleaks/gitleaks) to prevent accidental secret commits:

```bash
pip install pre-commit
cd core-oss
pre-commit install
```

After this, every `git commit` will automatically scan for secrets before allowing the commit.

### CI/CD

Every PR runs these checks automatically via GitHub Actions:

| Check | What it does |
|-------|-------------|
| **API: Lint** | Ruff linter on `api/` and `lib/` |
| **API: Type Check** | Mypy static type analysis |
| **API: Tests** | Pytest suite |
| **API: OpenAPI Schema** | Validates the generated OpenAPI spec |
| **Web: Build** | TypeScript check + Vite production build |
| **Secret Scanning** | Gitleaks scans for leaked credentials |

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

[Apache License 2.0](./LICENSE)
