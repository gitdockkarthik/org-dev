# CUR Analyser

AWS Cost & Usage Report analysis, cost anomaly detection and FinOps intelligence agent.

## 1. Overview

CUR Analyser is an AI-powered agent that ingests AWS Cost & Usage Report data and performs deep cost analysis across services, accounts, and time periods. It classifies cost anomalies, surfaces the top spend drivers, and generates actionable FinOps recommendations using Claude-powered analysis.

**AI capabilities:** cost breakdown by service and account, anomaly detection, top driver identification, savings opportunity analysis.

**Data sources supported:** file upload (CUR CSV export from AWS Billing), synthetic data (auto-generated for demos and testing), AWS Cost Explorer API (Phase 2).

**Phase roadmap:**
- **Phase 1** — Cost analysis, anomaly detection, top drivers dashboard, Settings UI (current)
- **Phase 2** — AWS live connectivity (Cost Explorer API + S3 CUR direct sync)
- **Phase 3** — FinOps RAG with AWS pricing documentation, Reserved Instance and Savings Plans guidance
- **Phase 4** — Autonomous FinOps actions (RI purchases, rightsizing, unused resource cleanup)

---

## 2. Architecture

- **Stateless FastAPI container** — no in-process state; all context injected per request
- **Own PostgreSQL database** — per-agent schema; isolated from platform and other agents
- **Own encrypted config storage** — Fernet-encrypted secrets stored in `agent_config` table
- **Optional platform registration** via `REGISTRY_URL` — agent starts and runs fully standalone if not set
- **Connects to AWS Cost Explorer API** (Phase 2) for live cost data sync
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
| `PORT` | No | `8002` | HTTP port the container listens on |

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
   DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/cur_analyser
   ENCRYPTION_KEY=<key from step 3>
   ```
5. Start the stack:
   ```bash
   docker compose up --build -d
   ```
6. Verify the agent is healthy:
   ```bash
   curl http://localhost:8002/health
   ```
7. Open the Settings UI:
   ```
   http://localhost:8002/ui/settings.html
   ```
8. Configure your Anthropic API key and select a data source.

---

## 5. EKS Deployment (SRE)

### Build and push image

```bash
docker build -t <ecr-repo>/cur-analyser:<version> .
docker push <ecr-repo>/cur-analyser:<version>
```

### Kubernetes manifests

**`deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cur-analyser
  namespace: <namespace>
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cur-analyser
  template:
    metadata:
      labels:
        app: cur-analyser
    spec:
      containers:
        - name: cur-analyser
          image: <ecr-repo>/cur-analyser:<version>
          ports:
            - containerPort: 8002
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: cur-analyser-secrets
                  key: database-url
            - name: ENCRYPTION_KEY
              valueFrom:
                secretKeyRef:
                  name: cur-analyser-secrets
                  key: encryption-key
            - name: REGISTRY_URL
              value: "http://platform-backend:8000"
          readinessProbe:
            httpGet:
              path: /health
              port: 8002
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8002
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
  name: cur-analyser
  namespace: <namespace>
spec:
  type: ClusterIP
  selector:
    app: cur-analyser
  ports:
    - port: 8002
      targetPort: 8002
```

### Deploy

```bash
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl rollout status deployment/cur-analyser -n <namespace>
```

### Verify

```bash
kubectl get pods -n <namespace>
kubectl logs <pod-name> -n <namespace>
```

---

## 6. Post-Deploy Configuration

1. Open the Settings UI at `http://<host>:8002/ui/settings.html`
   — or via the platform portal if using UAP/Operative
2. Enter your Anthropic API key
3. Select a data source (start with **Synthetic Data** to verify the agent works end-to-end before connecting real cost data)
4. Click **Save & Sync**
5. Verify the Sync Status section shows records loaded and total cost in window
6. Switch to **File Upload** with a real CUR CSV export when ready for live data
7. Set the **Cost Window** to Last 30 days (recommended for meaningful trend analysis)
8. Tune the anomaly detection threshold under the **Cost Tuning** tab if the default 20% doesn't match your environment
9. AWS Cost Explorer live connectivity is available in Phase 2

---

## 7. Data Sources

### Synthetic Data

No configuration required. Click **Save & Sync** with Synthetic Data selected in Settings. Generates realistic AWS cost data across common services (EC2, RDS, S3, Lambda, CloudFront, and others) with realistic spend distributions for demo and threshold tuning.

### File Upload (CUR CSV)

Upload via the Reports page in the portal, or POST directly:

```bash
POST /reports/upload
Content-Type: multipart/form-data
```

The uploaded file must contain at minimum:

| Column | Description |
|---|---|
| `line_item_product_code` | AWS service identifier |
| `line_item_unblended_cost` | Cost amount in USD |
| `line_item_usage_start_date` | Usage period start timestamp |

The agent auto-detects column names and tolerates minor format variations. To export from AWS: **Billing & Cost Management → Cost & Usage Reports → Download**.

### AWS Cost Explorer API (Phase 2)

Configure credentials in the **AWS Live** tab of the Settings UI once Phase 2 is available.

Required IAM permissions:
```
ce:GetCostAndUsage
ce:GetCostForecast
```

IAM Role is recommended over access keys for EKS deployments — configure the Role ARN in Settings and attach the policy to the pod's service account via IRSA.

---

## 8. Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Agent won't start | `DATABASE_URL` not set | Add to `.env` or K8s secret |
| Health check fails | Port mismatch | Verify `PORT=8002` and container exposes port 8002 |
| No data after sync | CUR CSV format wrong | Check that required columns exist in the uploaded file |
| Cost shows zero | Currency or column mismatch | Verify `line_item_unblended_cost` column contains non-zero values |
| AI responses empty | Anthropic key not configured | Add key via the Settings UI |
| Registration fails | `REGISTRY_URL` unreachable | Check platform backend is running. Agent works fully standalone without `REGISTRY_URL` |
| Large file upload fails | File exceeds 50 MB limit | Split the CUR export into smaller date-range files before uploading |

### Logs

```bash
# Local Docker
docker compose logs cur-analyser -f

# Kubernetes
kubectl logs -f deployment/cur-analyser -n <namespace>
```

### Health check

```bash
curl http://localhost:8002/health
# Expected: {"status": "ok", "agent": "cur-analyser"}
```

---

Built by AgentsIQ.
For issues: github.com/agentsiq/cur-analyser/issues
