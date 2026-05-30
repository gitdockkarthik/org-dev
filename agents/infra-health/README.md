# Infra Health Check Agent

A Claude-powered agent that answers questions about infrastructure service health, uptime, incidents, and remediation recommendations.

## Services monitored

| Service | Description |
|---------|-------------|
| `api-server` | Main application API gateway |
| `database` | Primary PostgreSQL cluster |
| `cache` | Redis cache layer |
| `queue` | Message queue (e.g. RabbitMQ / SQS) |
| `cdn` | Content delivery network |

## Deploying on Railway

### 1. Create a new Railway service

In your Railway project, click **New Service → GitHub Repo** and select this monorepo.

Set the following in the service settings:

| Setting | Value |
|---------|-------|
| Root Directory | `agents/infra-health/` |
| Dockerfile Path | `Dockerfile` |

### 2. Set environment variables

In the Railway service **Variables** tab, add:

```
ANTHROPIC_API_KEY=sk-ant-...
REGISTRY_URL=https://<your-backend>.up.railway.app
BACKEND_API_KEY=<your-backend-api-key>
```

All other variables are optional (defaults are sensible). See [.env.example](.env.example) for the full list.

### 3. Deploy

Railway will build the Dockerfile and start the service. The agent self-registers with the UAP backend on startup and is immediately available in the portal.

## Local development

```bash
# From the repo root
cp agents/infra-health/.env.example agents/infra-health/.env
# fill in ANTHROPIC_API_KEY

docker compose up infra-health
```

Or run directly:

```bash
cd agents/infra-health
pip install -r requirements.txt
uvicorn main:app --reload --port 8002
```

## API

### `GET /health`

```json
{"status": "ok", "agent": "infra-health"}
```

### `POST /invoke`

```json
{
  "session_id": "abc123",
  "user_message": "What is the current health of all services?",
  "context": {},
  "history": []
}
```

Response:

```json
{
  "session_id": "abc123",
  "response": "All services are healthy except **cache**, which is currently degraded...",
  "metadata": {"tokens_used": 312}
}
```

## Tool: `check_health`

The agent has one tool that returns live (mock) health data:

```json
{
  "checked_at": "2026-05-27T10:00:00+00:00",
  "overall_status": "degraded",
  "services": {
    "api-server": {
      "status": "healthy",
      "uptime_pct": 99.97,
      "response_time_ms": 42,
      "last_incident": "2026-05-14T03:12:00Z",
      "last_incident_summary": "...",
      "last_incident_age_hours": 312.5
    }
  }
}
```

To check specific services only, pass `"services": ["api-server", "database"]` in the tool input.
