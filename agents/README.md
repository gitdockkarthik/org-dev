# Agents

Each subfolder is a standalone FastAPI service that implements the agent invoke contract.

## Adding a new agent

1. Copy `alert-analyser/` as a template
2. Rename the folder to your agent's kebab-case product name (e.g. `cur-analyser`)
3. Update `manifest.json` with the correct name, slug, description, version, and capabilities
4. Implement your logic in `main.py` — the `SYSTEM_PROMPT` and `invoke` function are your main touch points
5. Add the agent to the platform by POSTing its manifest to `POST /api/v1/agents/register`

## Agent contract

```
GET  /health   → {"status": "ok", "agent": "<slug>", "version": "<semver>"}
POST /invoke   → InvokeRequest → InvokeResponse
```

See `shared/schemas.py` for the exact request/response shapes.

## Stateless rule

Agents **must not** store state between requests. All session context must arrive in `context` or `history` and be re-used within that single request only.
