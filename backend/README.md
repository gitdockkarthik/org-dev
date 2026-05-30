# Backend — Unified Agent Platform

FastAPI service providing the agent registry and orchestration layer.

## Modules

| Module | Prefix | Auth | Purpose |
|--------|--------|------|---------|
| `registry/` | `/api/registry` | API key required | Manage agent manifests, versioning, lifecycle |
| `orchestrator/` | `/api` | Mixed (see below) | Invoke agents, proxy portal reads, session history |

## Auth

All registry endpoints and `POST /api/invoke/*` require the header:
```
X-API-Key: <BACKEND_API_KEY>
```
`GET /api/agents`, `GET /api/agents/{slug}`, `GET /api/session/{id}`, and `GET /api/health` are public.

## Quick start

```bash
cp ../.env.example ../.env   # fill in secrets
make -C .. dev               # start postgres + backend
make -C .. migrate           # run initial migration
```

---

## Registry endpoints — curl examples

### Register an agent
```bash
curl -s -X POST http://localhost:8000/api/registry/agents \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-backend-api-key" \
  -d '{
    "name": "Alert Analyser",
    "slug": "alert-analyser",
    "description": "Triages cloud infrastructure alerts",
    "version": "0.1.0",
    "system_prompt": "You are an expert cloud infrastructure alert analyser.",
    "model": "claude-sonnet-4-6",
    "temperature": 0.3
  }' | jq
```

### List all agents (filter by status)
```bash
curl -s http://localhost:8000/api/registry/agents \
  -H "X-API-Key: your-backend-api-key" | jq

# filter
curl -s "http://localhost:8000/api/registry/agents?status=published" \
  -H "X-API-Key: your-backend-api-key" | jq
```

### Get agent by ID
```bash
curl -s http://localhost:8000/api/registry/agents/<agent-uuid> \
  -H "X-API-Key: your-backend-api-key" | jq
```

### Update an agent (auto-snapshots current version)
```bash
curl -s -X PUT http://localhost:8000/api/registry/agents/<agent-uuid> \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-backend-api-key" \
  -d '{
    "version": "0.2.0",
    "system_prompt": "Updated system prompt here.",
    "temperature": 0.5
  }' | jq
```

### Publish an agent
```bash
curl -s -X POST http://localhost:8000/api/registry/agents/<agent-uuid>/publish \
  -H "X-API-Key: your-backend-api-key" | jq
```

### Deprecate an agent
```bash
curl -s -X POST http://localhost:8000/api/registry/agents/<agent-uuid>/deprecate \
  -H "X-API-Key: your-backend-api-key" | jq
```

### List version history
```bash
curl -s http://localhost:8000/api/registry/agents/<agent-uuid>/versions \
  -H "X-API-Key: your-backend-api-key" | jq
```

---

## Orchestrator endpoints — curl examples

### Invoke an agent
```bash
curl -s -X POST http://localhost:8000/api/invoke/alert-analyser \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-backend-api-key" \
  -d '{
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_message": "CPU on prod-web-01 has been above 90% for 15 minutes.",
    "context": {
      "environment": "production",
      "region": "us-east-1"
    },
    "history": []
  }' | jq
```

### List published agents (portal-facing, no auth)
```bash
curl -s http://localhost:8000/api/agents | jq
```

### Get a published agent by slug (no auth)
```bash
curl -s http://localhost:8000/api/agents/alert-analyser | jq
```

### Get session history
```bash
curl -s http://localhost:8000/api/session/550e8400-e29b-41d4-a716-446655440000 | jq
```

### Health check
```bash
curl -s http://localhost:8000/api/health
```

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | ✓ | asyncpg DSN e.g. `postgresql+asyncpg://user:pass@host/db` |
| `ANTHROPIC_API_KEY` | ✓ | Anthropic API key for direct-invoke agents |
| `BACKEND_API_KEY` | ✓ | Shared secret for registry + invoke endpoints |
| `CORS_ORIGINS` | — | Comma-separated allowed origins (default: `http://localhost:3000`) |
| `BACKEND_PORT` | — | Port to bind (default: `8000`) |

## Migrations

```bash
# generate a new migration after model changes
docker compose run --rm backend alembic revision --autogenerate -m "description"

# apply all pending migrations
make migrate
```
