# agent-template

Copy-paste starting point for every new agent on the Unified Agent Platform.

---

## Quickstart — creating a new agent

### 1. Copy the template

```bash
cp -r agents/agent-template agents/my-agent-name
cd agents/my-agent-name
```

### 2. Fill in env vars

```bash
cp .env.example .env
```

Edit `.env`:

| Var | What to set |
|-----|-------------|
| `ANTHROPIC_API_KEY` | Your Anthropic key |
| `MODEL` | e.g. `claude-sonnet-4-6` |
| `AGENT_ID` | Generate once: `python -c "import uuid; print(uuid.uuid4())"` |
| `AGENT_SLUG` | kebab-case, matches folder name, e.g. `cur-analyser` |
| `AGENT_NAME` | Human-readable, e.g. `CUR Analyser` |
| `AGENT_SYSTEM_PROMPT` | The full system prompt — no code change needed |

### 3. Update manifest.json

Replace all placeholder values (`My Agent Name`, `my-agent-slug`, etc.) with
your real values. The `invoke_url` should be `http://<AGENT_SLUG>:<PORT>`.

### 4. Write your system prompt

Set `AGENT_SYSTEM_PROMPT` in `.env`. Keep all domain knowledge in this env var
so behaviour can be tuned per deployment without touching code.

### 5. Add or remove tools (optional)

**To add a tool**, create `tools/my_tool.py`:

```python
from typing import Any
from tools.base import ToolExecutor

class MyTool(ToolExecutor):
    name = "my_tool"
    description = "What this tool does."
    input_schema = {
        "type": "object",
        "properties": {
            "param": {"type": "string", "description": "A parameter."}
        },
        "required": ["param"],
    }

    async def execute(self, **kwargs: Any) -> str:
        param = kwargs["param"]
        # ... do something ...
        return f"Result for {param}"
```

Then register it in `main.py`:

```python
from tools.my_tool import MyTool
_runner = AgentRunner(tools=[EchoTool(), MyTool()])
```

**To remove the echo tool**, change `main.py` to:

```python
_runner = AgentRunner(tools=[])   # pure text, no tool use
```

**To disable tool use entirely**, pass an empty list — Claude will only respond
with text and the tool-use loop in `agent.py` is never entered.

### 6. Run locally

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

Test the health check:
```bash
curl http://localhost:8001/health
```

Test an invocation:
```bash
curl -s -X POST http://localhost:8001/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_message": "Hello, what can you do?",
    "context": {},
    "history": []
  }' | jq
```

### 7. Register with the platform

Once the agent is running (locally or deployed), POST its manifest to the backend:

```bash
curl -s -X POST http://localhost:8000/api/registry/agents \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-backend-api-key" \
  -d @manifest.json | jq
```

Then publish it so the orchestrator will accept invocations:

```bash
curl -s -X POST http://localhost:8000/api/registry/agents/<uuid>/publish \
  -H "X-API-Key: your-backend-api-key" | jq
```

---

## File reference

```
agent-template/
├── main.py         FastAPI app — schemas, /health, /invoke
├── agent.py        AgentRunner — Claude call + tool-use loop, stateless
├── config.py       All settings from env vars via pydantic-settings
├── tools/
│   ├── base.py     ToolExecutor ABC — subclass this for every tool
│   └── echo.py     Example tool — returns input unchanged
├── manifest.json   Agent descriptor — name, slug, invoke_url, tools list
├── Dockerfile      python:3.11-slim; PORT env var supported for Railway
├── requirements.txt Pinned dependencies
└── .env.example    All required env vars documented
```

---

## Architecture notes

**Stateless by design** — `AgentRunner.run()` takes the full `history` list on
every call and rebuilds messages from scratch. No in-memory session state.
The platform's orchestrator injects DB-backed history before forwarding.

**Tool-use loop** — if Claude responds with `stop_reason == "tool_use"`,
`agent.py` executes each requested tool, appends results, and calls Claude
again automatically. This repeats until Claude returns `end_turn`.

**System prompt is an env var** — `AGENT_SYSTEM_PROMPT` is read at startup
via pydantic-settings. Change behaviour without rebuilding the image by
updating the env var on Railway and redeploying.

**Delivered as a standalone repo** — this folder has no imports from the
monorepo's `shared/` package. Copy it out and it works independently.
