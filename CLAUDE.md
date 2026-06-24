# CLAUDE.md

Guidance for Claude Code when working in this repository. Read at the start of every session.

## Project: Operative Intelligence

A multi-agent platform that **reduces human intervention in operations** across four pillars:

- **Automation** — remove repetitive manual ops toil
- **Cost Optimisation** — surface and act on spend (e.g. `cur-analyser`)
- **Stability** — detect and triage reliability issues (e.g. `alert-analyser`, `kafka-analyser`)
- **Security** — harden and monitor posture

**`org-dev` is the development/test monorepo** — it runs the whole platform (backend, portal,
mcp-server, and all agents) together via Docker Compose so agents can be built and validated as an
integrated system. Each capability is built as a **pluggable standalone agent**: a self-contained
FastAPI service that connects to the Anthropic API and exposes a consistent chat + settings +
dashboard interface. Agents self-register with the backend on startup and appear in the portal
automatically. New agents are added by dropping a folder under `agents/` and adding one
`docker-compose.yml` block.

For production, each agent is **extracted and delivered to its own standalone repo on the org
Bitbucket** (see Delivery & Workflow below). Access is **VPN-only internal** — no public endpoints.

**Current agents (dev/test):** `alert-analyser`, `cur-analyser`, `kafka-analyser`
(plus `agent-template/` as a starter and `infra-health/` as a minimal example).

## Architecture

```
Browser (VPN)
   │
   ▼
portal (vanilla JS / nginx)  ── proxies /api/* ──►  backend (FastAPI orchestrator)
                                                       │  registry + invoke + auth + platform
                                                       │  X-API-Key (BACKEND_API_KEY)
                                                       ▼
                                            agents (pluggable FastAPI services)
                                              alert / cur / kafka
                                                       ▲
                                                       │ REST
                                            mcp-server (FastMCP / SSE gateway)

postgres ◄── backend + agents   (via data abstraction layer; Fernet-encrypted secrets)
shared/  ── common primitives imported across services
```

- **backend/** — FastAPI **orchestrator and control plane**. Owns the agent registry, the
  `/api/invoke/{slug}` flow, auth, and platform setup. Holds the Anthropic API key and injects it
  into agents per-request via the `X-Anthropic-Key` header. SQLAlchemy 2 async over PostgreSQL.
- **agents/** — pluggable standalone services. Each has `main.py` (app + lifespan +
  self-registration), `agent.py` (`AgentRunner` — Claude tool-use loop), `config.py`
  (Pydantic Settings incl. `agent_system_prompt`), `tools/` (each tool subclasses `ToolExecutor`),
  and `storage.py`/`database.py` (data layer). Endpoints: `GET /health`, `POST /invoke`
  (+ streaming variants), and dashboard/reports/settings routes.
- **portal/** — vanilla HTML/CSS/JS served by **nginx** (no framework, no build step). nginx
  proxies `/api/*` to the backend; `docker-entrypoint.sh` injects `BACKEND_URL`/`AUTH_MODE` via
  `envsubst` at container start. Includes the first-run `setup.html` wizard.
- **mcp-server/** — **FastMCP gateway** (SSE). Re-exposes each agent's REST API as MCP tools
  (`kafka_*`, `alert_*`, `cur_*`, `platform_health`) so external MCP clients can drive the agents.
  Holds no secrets; translates MCP calls → REST via `httpx` using `*_ANALYSER_URL` env vars.
- **shared/** — common primitives imported across services: `schemas.py`
  (`InvokeRequest`/`InvokeResponse` contract), `manifest.py` (`AgentManifest`), and
  `escalation/notifier.py` (Teams Adaptive Card escalation).
- **Data abstraction layer** — agents persist config/reports through a pluggable storage layer
  (`storage.py`/`database.py`) selected via `STORAGE_BACKEND` (e.g. `postgres`, `memory`) and
  `DATABASE_URL`, so an agent can run with or without Postgres.
- **Secrets management (done)** — all secrets are **Fernet-encrypted at rest** in PostgreSQL
  (`ENCRYPTION_KEY`); the Anthropic key is never stored in an agent and is **injected per request**.

### Repository topology
- **`unified-agent-platform`** (upstream monorepo, external) → source of truth that
  `sync-from-monorepo.sh` pulls *into* `org-dev`.
- **`org-dev`** (this repo) — the rebranded **dev/test integration monorepo** where the full
  platform runs together.
- **Org Bitbucket** (`bitbucket.operative.com/scm/ai/...`) — production delivery target. The model
  is **one standalone repo per agent**; local clones for delivery live under `operative-bitbucket/`.
  Note: the current state is transitional — the checked-out delivery clone points at a shared
  `sre-agents` repo with the agent in a subfolder, while the target is a dedicated repo per agent.

### Per-agent notes
- **alert-analyser** — OpsGenie / JSM alert noise detection, classification, suppression advice.
  Sources (`tools/source.py`): file (JSON/CSV), standalone OpsGenie, OpsGenie cloud, Atlassian JSM.
  Rule-based scoring in `tools/noise_detector.py`, tunable via `NOISE_THRESHOLD_*`.
- **cur-analyser** — AWS Cost & Usage Report analysis. In-memory SQL over CUR CSV via **DuckDB**
  (`tools/duckdb_engine.py`); synthetic generator + CSV upload.
- **kafka-analyser** — Kafka cluster health, consumer lag, broker/topic metrics, anomaly detection.
  Collectors: synthetic, real Kafka, Prometheus, Schema Registry, ZooKeeper, KafkaConnect, MirrorMaker.

All agents default to model `claude-sonnet-4-6` (override via `MODEL`). When touching Anthropic SDK
code, consult the `claude-api` skill rather than relying on memory for model IDs/params.

## Development Workflow

1. **Develop & test the full stack locally** on the **KPI box** (AL2023, Docker Compose):
   `make dev` brings up postgres + backend + agents + mcp-server + portal together.
2. **Test each standalone agent independently** before delivery — every agent is self-contained and
   serves its own `/invoke` + `/health` on its own port.
3. **Deliver the agent to its own Bitbucket repo** (production). One standalone repo per agent is
   the target model; delivery clones live under `operative-bitbucket/<agent>/`.

### Sync & delivery scripts
- **`sync-from-monorepo.sh`** syncs **inbound**: it copies `agents/`, `backend/`, `shared/`, and a
  selective `portal/` *from* the upstream `unified-agent-platform` monorepo (default
  `../unified-agent-platform`) *into* `org-dev`, strips Railway files, and re-applies OPERATIVE
  branding. It does **not** touch Bitbucket — its final step is `git push origin main` on `org-dev`.
- **Per-agent Bitbucket delivery has no script yet** — it is currently **manual**: work inside the
  relevant `operative-bitbucket/<agent>/` clone, copy the validated agent folder over, commit, and
  push to the agent's Bitbucket repo. (If you automate this, document it here.)

### Deployment
- **KPI box** (AL2023, Docker Compose) — the development and testing environment (this repo).
- **Org Bitbucket** — standalone per-agent repos are the production delivery target.
- **VPN-only** internal access throughout.

## Docker & Compose

`docker-compose.yml` is the source of truth: `postgres`, `backend`, `alert-analyser`,
`cur-analyser`, `kafka-analyser`, `mcp-server`, `portal`.

- **backend** bind-mounts `./backend:/app` and runs `uvicorn --reload` → code edits hot-reload.
  Agents and portal do **not** bind-mount; rebuild them to pick up changes.
- Only the **backend** receives `ANTHROPIC_API_KEY`; agents get it per-request via `X-Anthropic-Key`.
- Services talk by name on **internal** ports (`http://backend:8000`, `http://alert-analyser:8001`…);
  `REGISTRY_URL=http://backend:8000`.
- **Host port mappings come from `.env`** and differ from internal ports. Defaults: postgres
  `5433:5432`, backend `8010:8000`, alert `8001`, cur `8002`, kafka `8003`, mcp-server `8005`,
  portal `3000:80`. Trust `.env`/compose for host ports.
- `docker-compose.demo.yml` adds a real ZooKeeper + Kafka (confluentinc) for exercising
  kafka-analyser against a live cluster.

## Environment Configuration

Copy `.env.example` → `.env` (or run `./setup-env.sh`, which auto-generates secrets, chmod 600).
`.env` is gitignored. Must fill:
- `ENCRYPTION_KEY` — **Fernet key, critical.** Encrypts every secret in Postgres.
  `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
  **Back it up externally — if lost, encrypted data is unrecoverable.**
- `POSTGRES_PASSWORD` (`openssl rand -hex 16`) + matching `DATABASE_URL`
  (`postgresql+asyncpg://operative:<pw>@postgres:5432/operative_db`).
- `SECRET_KEY` (`python3 -c "import secrets; print(secrets.token_hex(32))"`) — JWT signing.
- `BACKEND_API_KEY` (`openssl rand -hex 32`) — `X-API-Key` shared secret (portal/agents ↔ backend).

Ports/auth: `*_PORT`, `AUTH_MODE` (`none` default / `local` / `okta`), `ADMIN_EMAIL`/`ADMIN_PASSWORD`
(local-mode seed), `PORTAL_USERNAME`/`PORTAL_PASSWORD`, `STORAGE_BACKEND`.

**Configured via the Portal UI, never in `.env`:** the Anthropic API key (setup wizard),
OpsGenie/JSM credentials, AWS credentials, and per-agent thresholds — all stored encrypted in Postgres.

## Key Commands (Makefile)

`-include .env` makes ports/keys available in recipes.

| Command | What it does |
|---|---|
| `make dev` | `docker compose up --build` — full stack on the KPI box |
| `make migrate` | `docker compose run --rm backend alembic upgrade head` |
| `make register` | Idempotently register + publish agents into the backend registry (needs `make dev` up) |
| `make test` | Smoke-test `/api/invoke/alert-analyser` and `/api/invoke/cur-analyser` through the backend |
| `make logs` | `docker compose logs -f` |

Also: `docker compose ps`, `docker compose logs -f <service>`, `docker compose up -d <agent>`,
`docker compose restart <agent>`. First run: `setup-env.sh` → `up -d postgres backend portal` →
wizard at `http://<host>:3000/setup.html` → `up -d <agents>`.

## Conventions & Patterns

- **FastAPI + Pydantic Settings everywhere**; system prompts live in each agent's `config.py`.
- **Secrets at rest are Fernet-encrypted** (`encryption.py` in backend and each agent). Keys matching
  `api_key`/`token`/`password`/`secret`/`url`/`cloud_id` are encrypted; decryption falls back to
  plaintext when `ENCRYPTION_KEY` is unset (dev convenience).
- **Two auth layers:** machine-to-machine `X-API-Key` (= `BACKEND_API_KEY`, via `require_api_key`);
  human auth via JWT cookie `operative_token` (HS256, 8h) when `AUTH_MODE=local` (default `none`).
- **Agents self-register on startup:** POST manifest to `{REGISTRY_URL}/api/registry/agents`, handle
  409 by matching on `slug`, then publish. No manual registry edits.
- **Tool pattern:** subclass `ToolExecutor` (`tools/base.py`), register in `AgentRunner.__init__`.
- **Adding an agent:** clone into `agents/<slug>/`, add a compose block (set `REGISTRY_URL`,
  `BACKEND_API_KEY`, `DATABASE_URL`, `ENCRYPTION_KEY`, `MODEL`, `AGENT_SLUG`),
  `docker compose up -d <slug>`. It appears in the portal automatically.

## Invoke Flow (agents ↔ backend ↔ portal)

1. Portal (nginx) proxies `/api/*` to the backend; bootstraps its key via `GET /api/platform/bootstrap`.
2. Portal calls `POST /api/invoke/{slug}` (`X-API-Key`). The orchestrator checks the agent is
   `published`, loads/creates the `ChatSession`, pulls recent history, then forwards the
   `InvokeRequest` (`session_id`, `user_message`, `context`, `history`) to the agent's `invoke_url`
   **with `X-Anthropic-Key`**.
3. The agent's `AgentRunner` runs a Claude tool-use loop, executes its tools against its data
   sources, and returns `InvokeResponse` (`response` + `metadata` incl. `tokens_used`, optional `chart`).
4. Backend persists user + assistant messages and returns to the portal.
5. Optional: agents escalate anomalies as Teams Adaptive Cards via `shared/escalation/notifier.py`.

---
*Internal use only — Engineering, Internal Platforms. Verify ports against `.env`/`docker-compose.yml`
and model IDs against the code, as both evolve.*

### cur-analyser: Data Source Abstraction & Inventory Enrichment (2026-06-22)

What was built:
- Pluggable data source abstraction layer under
  agents/cur-analyser/tools/data_sources/
- FileUploadCURProvider: CSV, CSV.zip, Parquet
- FileUploadInventoryProvider: 25-sheet XLSX,
  9 join keys, 10 enrichment fields
- DataSourceRegistry: Postgres persistence,
  archive support
- InventoryEnricher: resource-level + account-level
  fallback, ARN-tail matching, inv_* prefix
- Central resolve_col(): handles CUR 2.0, legacy,
  normalised, synthetic formats
- Two-CSV DBR zip: picks with-resources-and-tags variant

Feature flag: ENABLE_INVENTORY_ENRICHMENT=true in .env

Key decisions:
- inv_* columns are primary tag source, CUR fallback
- Account-level enrichment fallback when no ResourceId
- Single inventory enriches all CUR files
- Registry persists via Postgres across restarts

Settings UI: Settings → Data Source tab with CUR
Reports and Inventory File sections

Known gaps for next session:
- Real CUR validation pending — need non-anonymised
  CUR from inventory accounts (382638150166,
  516133168505, 433415038121, 517741648664)
- Large file: DuckDB file-path mode for 2GB+ files
- Performance: server-side pre-aggregation
- Delete button for CUR files in UI
- Data Quality recommendations panel

Commits: ~35 commits on main, all rollbackable

### cur-analyser: Settings & Performance improvements (2026-06-23)

What was built:
- CUR files grid in Settings with per-file match rate,
  resizable columns, copy filename, delete button
- Honest match rate — resource-level only,
  "Resource ID not in CUR" for legacy format
- Reports tab repurposed — removed upload/synthetic,
  added delete button, redirects to Settings
- Large file handling — file-path pipeline, DuckDB reads
  directly from disk, persistent Docker volume
- Server-side pre-aggregation — eliminated 200k row
  streaming to browser, loading bar gone
- Registry/report_store sync — single source of truth,
  startup reconciliation, symmetric delete
- Loading UX — spinner with progressive messages,
  per-tab skeleton screens, tab loading indicator

Production notes added to backlog:
- Storage: EBS volume for /app/data in production
- Phase 2 S3: DuckDB httpfs for direct S3 reads
- Rate limiting: max files, storage cap, upload queue

### cur-analyser: Filters, Performance & Comparison Mode (2026-06-23 afternoon)

What was built:
- Server-side dashboard filtering — all 7 base filters
  (Account, Environment, Service, Region, Pricing Term,
  Product Tag, Team Tag) now send filter params to
  GET /dashboard endpoint, DuckDB applies WHERE clause
- Filter UX fixes: 150ms debounce, sequence guard to
  prevent race conditions, chip removal syncs checkbox
  state and badge count, Reset clears all state cleanly
- DataFrame cache in duckdb_engine — 10min TTL keyed
  by file_path, per-file load lock prevents OOM on
  parallel cold-cache requests
- Parallel dashboard queries — compute_dashboard_async()
  runs all 17 queries concurrently via asyncio.gather +
  ThreadPoolExecutor(max_workers=8), server compute
  time reduced to ~0.74s
- Enhanced comparison mode — all panels now show with
  per-side "Not available" instead of blocking everything,
  added Service, Account, Environment, Region, Tag
  Coverage, Pricing Term panels side by side
- Removed [df-cache] diagnostic logs from
  duckdb_engine.py and routes_dashboard.py

Performance notes:
- Server compute: 0.74s (unfiltered), 0.33s (filtered)
- Perceived 10s over VPN is network latency (~350ms RTT)
  not server performance — will be faster on LAN
- Cache hits confirmed — all subsequent requests
  serve from in-memory DataFrame

Known gaps for next session:
- Comparison mode: resolve slash-format columns for
  rich-matched-cur-2026.csv left panel
- Enriched filters (Application, Budget Code, Customer)
  not yet wired to server-side filtering
- Combine multiple dashboard requests into single
  request to reduce VPN round trips

### alert-analyser: Deduplication & Period Filter Fix (2026-06-24)

What was built:
- Alert deduplication by alias — same incident appearing 
  multiple times (open + close events) now counted once.
  genuine_count dropped from 25,639 to 19,932 (5,707 dupes removed)
- Dedup info shown on all KPI cards — each card shows
  "(X total · Y dupes removed)" when duplicates exist:
  Total Alerts, Noise, Noise Suspect, Genuine (Row 1)
  New Alerts, New Noise, New Suspect, New Genuine (Row 2)
- Period filter consistency fix — Row 2 KPI cards now 
  use the same date range and dataset as all other tabs.
  Previously Row 2 used synced_at from alert_report_summary
  while other tabs used createdAt from alert list.
- get_dashboard() and get_period_summary() both now:
  - Use latest report regardless of sync time
  - Filter alerts by createdAt using shared helper
  - Return consistent counts across all tabs
- Row 2 label changed from "New Alerts (No syncs)" to 
  "Alerts in period" when date filter is active
- Comparison mode tested and working correctly

Key fixes:
- _filter_alerts_by_date() shared helper handles T-separator
  in date params (2026-06-24T06:46 format)
- noData check uses new_alerts presence not sync_count
  (sync_count=0 is valid for createdAt filter path)
- Stats cache backfill ensures new dedup fields available
  without waiting for next sync

Pending (requires team adoption):
- Lifecycle notification filtering (e.g. [Closed], [Resolved])
  pending team adopting [Tool] [State] [Severity] Message pattern
- Maintenance window filtering ([Maintenance] prefix)
  same dependency on pattern adoption
