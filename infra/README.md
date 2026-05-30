# Infra

## Production: Railway

Railway is the permanent deployment platform. Each service maps to one Railway service within a shared project.

### Services

| Railway service | Source path | Port |
|----------------|-------------|------|
| `postgres` | Railway managed | 5432 |
| `backend` | `backend/` | 8000 |
| `alert-analyser` | `agents/alert-analyser/` | 8001 |

### Deploying a new agent

1. Create a new Railway service pointed at the agent's subfolder
2. Set `ANTHROPIC_API_KEY` and any agent-specific env vars in the service settings
3. Set `DATABASE_URL` if the agent needs DB access (most don't — they're stateless)
4. After deploy, POST the agent's `manifest.json` to `POST /api/v1/agents/register` on the backend

### Environment variables

All secrets are configured in Railway's dashboard per-service. Never commit `.env` files.

## Local: Docker Compose

See the root `docker-compose.yml` and `Makefile` for local dev.

## Future: Kubernetes

Reserved for `k8s/` manifests if Railway is outgrown. Not in scope yet.
