# CLAUDE.md

Guidance for Claude Code when working in this repository.

## 1. Project Overview

**Operative Intelligence — Dev Platform** is a self-hosted AI agent platform for internal
engineering and operations teams. It runs on a single Docker host inside a VPN and provides a
browser portal where teams chat with specialised AI agents, view dashboards, and configure data
sources. No public endpoints are exposed.

Each **agent** is a standalone FastAPI service that connects to the Anthropic API and exposes a
consistent chat + settings + dashboard interface. The platform is plugin-style: a new agent is
added by dropping a folder under `agents/` and adding one service block to `docker-compose.yml`.
Agents self-register with the backend on startup and then appear in the portal automatically.

Three agents ship today:
- **alert-analyser** — OpsGenie / JSM alert noise detection, classification, and suppression advice
- **cur-analyser** — AWS Cost & Usage Report (CUR) analysis via DuckDB
- **kafka-analyser** — Kafka cluster health, consumer lag, broker/topic metrics, anomaly detection

Plus `agent-template/` (starter) and `infra-health/` (minimal example).

**Stack:** FastAPI (Python 3.11) · PostgreSQL 15 + SQLAlchemy 2 async · vanilla HTML/JS portal
served by nginx · Anthropic SDK · Fernet-encrypted secrets · Docker Compose (local) / Railway (prod).

## 2. Architecture & Key Components

```
Browser (VPN)
   │
   ▼
portal (nginx :3000→80)  ── proxies /api/* ──►  backend (FastAPI :8000)
                                                   │  ▲ registry + orchestrator
                                                   │  │ X-API-Key (BACKEND_API_KEY)
                                                   ▼  │
                                        agents (FastAPI :8001/8002/8003)
                                          alert / cur / kafka
                                                   ▲
                                                   │ REST
                                        mcp-server (FastMCP/SSE :8005)
                                          exposes agents as MCP tools

postgres (:5432, host :5433) ◄── backend + every agent (encrypted secrets, chat, reports)
```

### backend/ — platform orchestrator (FastAPI, Python 3.11)
The control plane. Holds the Anthropic API key and forwards it to agents per-request via the
`X-Anthropic-Key` header. Key layout:
- [main.py](backend/main.py) — app, CORS middleware, lifespan (create tables → verify DB → seed
  `platform_config` from env → seed admin user when `AUTH_MODE=local`).
- `core/` — `config.py` (Pydantic Settings), `database.py` (async engine, `AsyncSessionLocal`,
  `Base`), `auth.py` (JWT HS256 + bcrypt), `encryption.py` (Fernet), `security.py`
  (`require_api_key` for `X-API-Key`), `platform_cache.py` (in-memory key cache).
- `models/` — `agents`, `agent_versions`, `operative_users`, `chat_sessions`, `chat_messages`,
  `platform_config`. UUID PKs; `tools`/`config_snapshot` are JSONB.
- `registry/router.py` — agent CRUD + publish/deprecate + version snapshots, mounted at
  `/api/registry` (requires `X-API-Key`).
- `orchestrator/router.py` — `/api/invoke/{slug}`, public `/api/agents`, `/api/session/{id}`,
  `/api/health`, and a `/api/agents/{slug}/proxy/{path}` forwarder. `orchestrator/platform.py` —
  `/api/platform/*` setup/bootstrap. `orchestrator/mcp_client.py` — routes proxy calls through MCP.
- `routes_auth.py` — `/api/auth/*` (login/logout/me) and `/api/admin/users/*`.
- `migrations/` — Alembic (`0001_initial.py`). Note: startup also calls
  `Base.metadata.create_all()`, so a fresh DB works without running migrations.

### agents/ — standalone agent services (FastAPI)
All agents share the same template. Common files per agent: `main.py` (app + lifespan +
self-registration), `agent.py` (`AgentRunner` — Claude invocation + tool-use loop), `config.py`
(Pydantic Settings incl. the `agent_system_prompt`), `tools/` (each tool subclasses
`ToolExecutor` in `tools/base.py`), `models.py` + `database.py` + `storage.py` + `encryption.py`
(per-agent Postgres config/report storage), `routes_settings.py` / `routes_dashboard.py` /
`routes_reports.py`, `manifest.json`.

Endpoints every agent exposes: `GET /health`, `POST /invoke` (+ streaming variants
`/invoke/stream`, `/invoke/stream-insights`), plus dashboard/reports/settings routes.

- **alert-analyser** (:8001) — model `claude-sonnet-4-6`. Data sources in `tools/source.py`:
  File (JSON/CSV), standalone OpsGenie, OpsGenie cloud, Atlassian JSM. Rule-based noise scoring in
  `tools/noise_detector.py` (tunable via `NOISE_THRESHOLD_*`). Stores `AlertReport` rows in Postgres.
- **cur-analyser** (:8002) — model `claude-sonnet-4-6`. Uses **DuckDB** (`tools/duckdb_engine.py`)
  for in-memory SQL over CUR CSV. Synthetic generator + CSV upload. Needs `gcc` at build (pandas/duckdb).
- **kafka-analyser** (:8003) — model `claude-sonnet-4-6`. Collectors: synthetic, real Kafka,
  Prometheus, Schema Registry, ZooKeeper, KafkaConnect, MirrorMaker. Time-series `kafka_metrics_history`
  table. Dockerfile installs a JRE (`jpype1`/`kafka-python-ng`).

### portal/ — browser UI (static, nginx)
Vanilla HTML/CSS/JS — no framework, no build step. Pages: `index.html` (agent catalogue),
`setup.html` (first-run wizard), `chat.html`, `login.html`, per-agent
`agents/{slug}/{dashboard,settings,reports}.html`, and `admin/`. nginx proxies `/api/*` →
`http://backend:8000/api/`. A `docker-entrypoint.sh` runs `envsubst` over `js/config.template.js`
to inject `BACKEND_URL` / `AUTH_MODE` at container start. Container serves on port 80.

### mcp-server/ — MCP gateway (FastMCP, SSE, :8005)
Single [server.py](mcp-server/server.py). Wraps each agent's REST API as MCP tools (e.g.
`kafka_cluster_overview`, `alert_dashboard`, `cur_dashboard`, `platform_health`) using
`httpx.AsyncClient`. Transport is SSE (`mcp.run(transport="sse")`). Agent URLs come from
`KAFKA_ANALYSER_URL` / `ALERT_ANALYSER_URL` / `CUR_ANALYSER_URL`. Lets external MCP clients drive
the agents with zero agent-side changes.

### shared/ — small shared Python package
`schemas.py` (`InvokeRequest` / `InvokeResponse` contract), `manifest.py` (`AgentManifest`),
`escalation/notifier.py` (Teams Adaptive Card builder + webhook escalation with severity filtering
and cooldown). Deliberately minimal — agents are otherwise self-contained.

### infra/ — deployment notes
README only (no manifests yet). Production target is **Railway** (one service per component);
local dev is Docker Compose; Kubernetes is reserved for the future.

### Other top-level dirs
- `config/` — `kafka-sasl-jaas.conf` (gitignored; for the demo Kafka stack).
- `scripts/` — `demo-kafka-seed.sh`.
- `images/`, `operative-bitbucket/` — assets / vendored integration.

## 3. Docker & Docker Compose

`docker-compose.yml` is the source of truth for services: `postgres`, `backend`,
`alert-analyser`, `cur-analyser`, `kafka-analyser`, `mcp-server`, `portal`.

Key facts:
- **backend** mounts `./backend:/app` and runs `uvicorn main:app --reload` — code edits hot-reload
  without a rebuild. Agents and portal do **not** bind-mount; rebuild them to pick up changes.
- Only the **backend** receives `ANTHROPIC_API_KEY`. Agents get the key per-request via the
  `X-Anthropic-Key` header injected by the orchestrator (direct `make test` calls must supply it).
- Inside the Docker network services talk by name on internal ports
  (`http://backend:8000`, `http://alert-analyser:8001`, etc.); `REGISTRY_URL` is `http://backend:8000`.
- **Host port mappings come from `.env` and differ from internal ports.** Defaults:
  postgres `5433:5432`, backend `8010:8000`, alert `8001`, cur `8002`, kafka `8003`,
  mcp-server `8005`, portal `3000:80`. (The README's `:8000`/`:5432` references are the older
  internal values — trust `.env` / compose for host ports.)
- `docker-compose.demo.yml` spins up a real ZooKeeper + Kafka (confluentinc images) for exercising
  kafka-analyser against a live cluster.

## 4. Environment Configuration

Copy `.env.example` → `.env` (or run `./setup-env.sh`, which auto-generates secrets and chmods 600).
`.env` is gitignored. Variables you must fill:
- `ENCRYPTION_KEY` — **Fernet key, critical.** Encrypts every secret in Postgres. Generate with
  `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
  **Back it up externally — if lost, all encrypted data is unrecoverable.**
- `POSTGRES_PASSWORD` (`openssl rand -hex 16`), and matching `DATABASE_URL`
  (`postgresql+asyncpg://operative:<pw>@postgres:5432/operative_db`).
- `SECRET_KEY` (`python3 -c "import secrets; print(secrets.token_hex(32))"`) — JWT signing.
- `BACKEND_API_KEY` (`openssl rand -hex 32`) — the `X-API-Key` shared secret between portal/agents
  and the backend.

Ports/auth: `POSTGRES_PORT`, `BACKEND_PORT`, `PORTAL_PORT`, `ALERT_/CUR_/KAFKA_ANALYSER_PORT`,
`AUTH_MODE` (`none` default / `local` / `okta`), `ADMIN_EMAIL` + `ADMIN_PASSWORD` (local mode seed),
`PORTAL_USERNAME` / `PORTAL_PASSWORD`.

**Configured via the Portal UI, never in `.env`:** the Anthropic API key (entered in the setup
wizard), OpsGenie/JSM credentials, AWS credentials, and all per-agent thresholds — all stored
**encrypted in PostgreSQL**.

## 5. Key Development Commands (Makefile)

The Makefile `-include .env` so ports/keys are available in recipes.

| Command | What it does |
|---|---|
| `make dev` | `docker compose up --build` — start the whole stack |
| `make migrate` | `docker compose run --rm backend alembic upgrade head` |
| `make register` | Idempotently register + publish alert-analyser and cur-analyser into the backend registry (requires `make dev` running) |
| `make test` | Smoke-test `/api/invoke/alert-analyser` then `/api/invoke/cur-analyser` through the backend |
| `make logs` | `docker compose logs -f` |

Other useful commands: `docker compose ps`, `docker compose logs -f <service>`,
`docker compose restart <agent>`, `docker compose up -d <agent>`.
First-run flow (see README): `setup-env.sh` → `docker compose up -d postgres backend portal` →
setup wizard at `http://<host>:3000/setup.html` → `docker compose up -d <agents>`.
`sync-from-monorepo.sh` pulls agent code from an upstream monorepo and strips Railway files.

## 6. Conventions & Patterns

- **FastAPI + Pydantic Settings everywhere.** Config is a `Settings` class reading from env/`.env`.
  System prompts live in each agent's `config.py` as `agent_system_prompt`.
- **Secrets at rest are Fernet-encrypted.** `core/encryption.py` (backend) and each agent's
  `encryption.py` encrypt any config key matching `api_key`/`token`/`password`/`secret`/`url`/
  `cloud_id`. Decryption falls back to plaintext when `ENCRYPTION_KEY` is unset (dev convenience).
- **Two auth layers:** machine-to-machine via `X-API-Key` (= `BACKEND_API_KEY`, enforced by
  `require_api_key`), and human auth via JWT cookie `operative_token` (HS256, 8h) when
  `AUTH_MODE=local`. `AUTH_MODE=none` is the default.
- **The Anthropic key never lives in an agent.** Backend resolves it from its cache and injects
  `X-Anthropic-Key`; `AgentRunner.run()` uses that header, falling back to env only for direct dev calls.
- **Agents self-register on startup** — POST manifest to `{REGISTRY_URL}/api/registry/agents`
  (auth via `/api/platform/agent-token` or `BACKEND_API_KEY`), handle 409 by matching on `slug`,
  then publish. No manual registry edits needed.
- **Tool pattern:** subclass `ToolExecutor` (`tools/base.py`), register in `AgentRunner.__init__`,
  expose via the agent's tool-use loop.
- **Adding an agent:** clone into `agents/<slug>/`, add a compose service block (set
  `REGISTRY_URL`, `BACKEND_API_KEY`, `DATABASE_URL`, `ENCRYPTION_KEY`, `MODEL`, `AGENT_SLUG`),
  `docker compose up -d <slug>`. It appears in the portal automatically.
- Models default to `claude-sonnet-4-6` (overridable via `MODEL`). When touching Anthropic SDK code,
  consult the `claude-api` skill rather than relying on memory for model IDs/params.

## 7. How agents, backend & portal interact

1. **Portal → Backend.** Browser hits the portal (nginx); nginx proxies `/api/*` to the backend.
   The frontend bootstraps the platform API key via `GET /api/platform/bootstrap`.
2. **Chat invoke.** Portal calls `POST /api/invoke/{slug}` (with `X-API-Key`). The orchestrator
   validates the agent is `published`, loads/creates the `ChatSession`, pulls recent history, then
   forwards the `InvokeRequest` (`session_id`, `user_message`, `context`, `history`) to the agent's
   `invoke_url` **with the `X-Anthropic-Key` header** (or calls Anthropic directly if no `invoke_url`).
3. **Agent → Claude.** `AgentRunner` runs a tool-use loop against Claude using the injected key,
   executes its tools (querying its own data sources / Postgres), and returns `InvokeResponse`
   (`response`, `metadata` incl. `tokens_used` and an optional parsed `chart`).
4. **Persistence.** Backend writes user + assistant messages to `chat_messages` and returns the
   result to the portal. Dashboards/reports/settings are served by the agent's own routes (often
   surfaced through the backend proxy or directly).
5. **Escalation (optional).** Agents can push Teams Adaptive Cards via `shared/escalation/notifier.py`.

## 8. MCP Server

`mcp-server/` is an independent **FastMCP** gateway (SSE, port 8005) that re-exposes the agents'
REST endpoints as MCP tools so external MCP-aware clients can drive them. It holds no secrets and
makes no agent-side changes — it simply translates MCP tool calls into `httpx` REST calls against
`KAFKA_ANALYSER_URL` / `ALERT_ANALYSER_URL` / `CUR_ANALYSER_URL`. Tools are namespaced per agent
(`kafka_*`, `alert_*`, `cur_*`) plus a `platform_health` check. It depends on the three agents in
compose and is separate from the backend's internal `orchestrator/mcp_client.py` (which routes the
backend's own proxy calls).

---
*Internal use only — Engineering, Internal Platforms. Verify ports against `.env`/`docker-compose.yml`
and model IDs against the actual code, as both evolve.*
