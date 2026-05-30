# Alert Analyser

OpsGenie and JSM alert noise detection, suppression intelligence and escalation agent.

## 1. Overview

Alert Analyser is an AI-powered agent that ingests OpsGenie and JSM alert streams and classifies each alert as genuine or noise using configurable scoring heuristics and Claude-powered analysis. It surfaces suppression recommendations, identifies repeat offenders, and generates trend reports to reduce alert fatigue across on-call teams.

**AI capabilities:** noise classification, suppression recommendations, trend analysis.

**Data sources supported:** OpsGenie/JSM API (live sync via Atlassian JSM Ops API), file upload (JSON or CSV export), synthetic data (auto-generated for demos and testing).

**Phase roadmap:**
- **Phase 1** — Noise detection, suppression advisor, dashboard, Settings UI (current)
- **Phase 2** — MS Teams escalation + HITL approval workflows
- **Phase 3** — RAG-grounded analysis using runbooks, Jira incidents, and Confluence
- **Phase 4** — Autonomous remediation with configurable blast radius and spend limits

---

## 2. Architecture

- **Stateless FastAPI container** — no in-process state; all context injected per request
- **Own PostgreSQL database** — per-agent schema; isolated from platform and other agents
- **Own encrypted config storage** — Fernet-encrypted secrets stored in `agent_config` table
- **Optional platform registration** via `REGISTRY_URL` — agent starts and runs fully standalone if not set
- **Connects to OpsGenie/JSM** via the Atlassian JSM Ops API using Basic Auth (email + API token)
- **Standard `/invoke` contract** — compatible with any orchestrator or the UAP platform backend
- **Own Settings UI** at `/ui/settings.html` — fully self-contained, no platform dependency required

---

## 3. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `ENCRYPTION_KEY` | Yes | — | Fernet key for encrypting stored secrets |
| `ANTHROPIC_API_KEY` | No | — | Can be set via Settings UI after startup |
| `MODEL` | No | `claude-sonnet-4-6` | Claude model used for inference |
| `REGISTRY_URL` | No | — | Platform backend URL for self-registration. Agent runs standalone if not set |
| `BACKEND_API_KEY` | No | — | Legacy fallback if platform token fetch fails |
| `PORT` | No | `8001` | HTTP port the container listens on |
| `NOISE_THRESHOLD_REPEAT` | No | `3` | Aliases firing more than N times/hour flagged as noise |
| `NOISE_THRESHOLD_CLOSE_SECS` | No | `300` | Alerts auto-closing in fewer than N seconds flagged as noise |

---

## 4. Docker — Dev Setup

1. Clone this repo
2. Copy the example env file:
   ```bash
   cp .env.example .env
   ```
3. Generate an encryption key:
   ```bash
   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
4. Add the following to `.env`:
   ```
   DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/alert_analyser
   ENCRYPTION_KEY=<key from step 3>
   ```
5. Start the stack:
   ```bash
   docker compose up --build -d
   ```
6. Verify the agent is healthy:
   ```bash
   curl http://localhost:8001/health
   ```
7. Open the Settings UI:
   ```
   http://localhost:8001/ui/settings.html
   ```
8. Configure your Anthropic API key and select a data source.

---

## 5. EKS Deployment (SRE)

### Build and push image

```bash
docker build -t <ecr-repo>/alert-analyser:<version> .
docker push <ecr-repo>/alert-analyser:<version>
```

### Kubernetes manifests

**`deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: alert-analyser
  namespace: <namespace>
spec:
  replicas: 1
  selector:
    matchLabels:
      app: alert-analyser
  template:
    metadata:
      labels:
        app: alert-analyser
    spec:
      containers:
        - name: alert-analyser
          image: <ecr-repo>/alert-analyser:<version>
          ports:
            - containerPort: 8001
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: alert-analyser-secrets
                  key: database-url
            - name: ENCRYPTION_KEY
              valueFrom:
                secretKeyRef:
                  name: alert-analyser-secrets
                  key: encryption-key
            - name: REGISTRY_URL
              value: "http://platform-backend:8000"
          readinessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 15
            periodSeconds: 30
          resources:
            requests:
              cpu: "100m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "1Gi"
```

**`service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: alert-analyser
  namespace: <namespace>
spec:
  type: ClusterIP
  selector:
    app: alert-analyser
  ports:
    - port: 8001
      targetPort: 8001
```

### Deploy

```bash
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl rollout status deployment/alert-analyser -n <namespace>
```

### Verify

```bash
kubectl get pods -n <namespace>
kubectl logs <pod-name> -n <namespace>
```

---

## 6. Post-Deploy Configuration

1. Open the Settings UI at `http://<host>:8001/ui/settings.html`
   — or via the platform portal if using UAP/Operative
2. Enter your Anthropic API key
3. Select a data source (start with **Synthetic Data** to verify the agent works end-to-end before connecting real systems)
4. Click **Save & Sync**
5. Verify the Sync Status section shows alerts fetched
6. Switch to **OpsGenie / JSM API** when ready for live data
7. Enter your Cloud ID, Email, and API Token
8. Set a Sync Schedule for automated periodic updates
9. Tune Noise Detection thresholds under the **Noise Tuning** tab if defaults don't match your alert patterns

---

## 7. Data Sources

### Synthetic Data

No configuration required. Click **Save & Sync** with Synthetic Data selected in Settings, or POST directly:

```bash
POST /reports/generate-sample
```

Generates 200 realistic alerts covering a range of priorities, services, and noise patterns — sufficient for demo and threshold tuning.

### OpsGenie / JSM API

Configure the following fields via the Settings UI:

| Field | Where to find it |
|---|---|
| **Cloud ID** | Atlassian Admin → Products → JSM Ops → API settings |
| **Email** | Your Atlassian account email address |
| **API Token** | `id.atlassian.com/manage-profile/security/api-tokens` |

**Required permission:** Service Desk Agent role or higher on the JSM project.

Sync fetches all alerts within the configured time window using cursor-based pagination (50 alerts per page). Subsequent syncs are incremental — only alerts created after `last_synced` are fetched.

### File Upload

Upload via the Reports page in the portal, or POST directly:

```bash
POST /reports/upload
Content-Type: multipart/form-data
```

Accepts:
- **JSON** — array of OpsGenie alert objects
- **CSV** — export from OpsGenie Reports (standard column format)

---

## 8. Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Agent won't start | `DATABASE_URL` not set | Add to `.env` or K8s secret |
| Health check fails | Port mismatch | Check `PORT` env var matches the container port |
| No alerts after sync | OpsGenie credentials wrong | Verify Cloud ID, email, and token in Settings |
| Sync fetches 0 alerts | Time window too short | Increase the sync window in Settings |
| AI responses empty | Anthropic key not configured | Add key via the Settings UI |
| Registration fails | `REGISTRY_URL` unreachable | Check platform backend is running. Agent works fully standalone without `REGISTRY_URL` |
| Container restart loop | DB connection failed | Verify `DATABASE_URL` and confirm PostgreSQL is healthy |

### Logs

```bash
# Local Docker
docker compose logs alert-analyser -f

# Kubernetes
kubectl logs -f deployment/alert-analyser -n <namespace>
```

### Health check

```bash
curl http://localhost:8001/health
# Expected: {"status": "ok", "agent": "alert-analyser"}
```

---

Built by AgentsIQ.
For issues: github.com/agentsiq/alert-analyser/issues
