# Kafka Analyser

Kafka cluster health monitoring, consumer lag detection, broker metrics and anomaly intelligence agent.

## 1. Overview

Kafka Analyser is an AI-powered agent that collects Kafka cluster metrics and performs deep health analysis across brokers, consumer groups, topics, and Kafka Connect connectors. It detects consumer lag growth, broker heap pressure, under-replicated partitions, and connector failures using configurable thresholds and Claude-powered analysis.

**AI capabilities:** cluster health scoring, consumer lag analysis, broker metric interpretation, connector status monitoring, anomaly detection with ranked recommendations.

**Data sources supported:** synthetic data (auto-generated realistic cluster snapshot for demos and testing), Redpanda Cloud free tier (Phase 2), self-hosted Apache Kafka via JMX (Phase 3), AWS MSK (Phase 3).

**Phase roadmap:**
- **Phase 1** — Synthetic cluster data, anomaly detection, dashboard, Settings UI (current)
- **Phase 2** — Redpanda Cloud connectivity (real Kafka protocol, zero cost)
- **Phase 3** — Enterprise Kafka via JMX Exporter and AWS MSK
- **Phase 4** — RAG-grounded analysis using pgvector incident embeddings and Qdrant runbook store
- **Phase 5** — Autonomous remediation (consumer group restarts, connector restarts, partition scaling)

---

## 2. Architecture

- **Stateless FastAPI container** — no in-process state; all context injected per request
- **Shared PostgreSQL database** — same instance as alert-analyser and cur-analyser; kafka-specific tables (`kafka_clusters`, `kafka_broker_metrics`, `kafka_consumer_lag`, `kafka_topic_metrics`, `kafka_connector_status`, `kafka_anomalies`) are additive and created on first startup
- **Own encrypted config storage** — Fernet-encrypted secrets stored in `agent_config` table, scoped by `agent_slug = 'kafka-analyser'`
- **Optional platform registration** via `REGISTRY_URL` — agent starts and runs fully standalone if not set
- **Connects to Kafka clusters** via Redpanda Cloud API or JMX Exporter (Phase 2/3)
- **Standard `/invoke` contract** — compatible with any orchestrator or the UAP platform backend
- **Own Settings UI** at `/ui/settings.html` — fully self-contained, no platform dependency required

---

## 3. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (shared with other agents) |
| `ENCRYPTION_KEY` | Yes | — | Fernet key for encrypting stored secrets |
| `ANTHROPIC_API_KEY` | No | — | Can be set via Settings UI after startup |
| `MODEL` | No | `claude-sonnet-4-6` | Claude model used for inference |
| `REGISTRY_URL` | No | — | Platform backend URL for self-registration. Agent runs standalone if not set |
| `BACKEND_API_KEY` | No | — | Legacy fallback if platform token fetch fails |
| `PORT` | No | `8003` | HTTP port the container listens on |

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
4. Add the following to the root `.env` (shared with other agents):
   ```
   DATABASE_URL=postgresql+asyncpg://user:password@postgres:5432/agentsiq
   ENCRYPTION_KEY=<key from step 3>
   ```
5. Start the stack (includes postgres shared by all agents):
   ```bash
   docker compose up --build -d
   ```
6. Verify the agent is healthy:
   ```bash
   curl http://localhost:8003/health
   ```
7. Open the Settings UI:
   ```
   http://localhost:8003/ui/settings.html
   ```
8. Configure your Anthropic API key, select Synthetic Data, and click Save & Sync.

---

## 5. EKS Deployment (SRE)

### Build and push image

```bash
docker build -t <ecr-repo>/kafka-analyser:<version> .
docker push <ecr-repo>/kafka-analyser:<version>
```

### Kubernetes manifests

**`deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-analyser
  namespace: <namespace>
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-analyser
  template:
    metadata:
      labels:
        app: kafka-analyser
    spec:
      containers:
        - name: kafka-analyser
          image: <ecr-repo>/kafka-analyser:<version>
          ports:
            - containerPort: 8003
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kafka-analyser-secrets
                  key: database-url
            - name: ENCRYPTION_KEY
              valueFrom:
                secretKeyRef:
                  name: kafka-analyser-secrets
                  key: encryption-key
            - name: REGISTRY_URL
              value: "http://platform-backend:8000"
          readinessProbe:
            httpGet:
              path: /health
              port: 8003
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8003
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
  name: kafka-analyser
  namespace: <namespace>
spec:
  type: ClusterIP
  selector:
    app: kafka-analyser
  ports:
    - port: 8003
      targetPort: 8003
```

### Deploy

```bash
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl rollout status deployment/kafka-analyser -n <namespace>
```

### Verify

```bash
kubectl get pods -n <namespace>
kubectl logs <pod-name> -n <namespace>
```

---

## 6. Post-Deploy Configuration

1. Open the Settings UI at `http://<host>:8003/ui/settings.html`
   — or via the platform portal if using UAP/Operative
2. Enter your Anthropic API key
3. Select **Synthetic Data** (always works — no cluster needed) to verify the agent end-to-end
4. Click **Save & Sync**
5. Verify the Sync Status section shows brokers, consumer groups, topics, and connectors loaded
6. Switch to **Redpanda Cloud** when ready for live data (Phase 2)
7. Enter Bootstrap Servers, SASL Username, SASL Password, and toggle TLS if required
8. Set a Collection Interval for automated periodic updates
9. Tune Alert Thresholds under the **Alert Thresholds** tab to match your environment

---

## 7. Data Sources

### Synthetic Data

No configuration required. Click **Save & Sync** with Synthetic Data selected, or POST directly:

```bash
POST /reports/generate-sample
```

Generates a realistic `prod-kafka-cluster` snapshot with 3 brokers, 5 consumer groups, 10 topics, and 2 connectors. Includes intentionally injected anomalies:

| Anomaly | Severity |
|---|---|
| broker-2 heap at 78% with GC pressure | Warning |
| 3 under-replicated partitions on broker-2 | Critical |
| checkout-service lag at 45,200 and growing | Critical |
| s3-sink-audit connector FAILED (2/3 tasks) | Critical |
| audit-log topic at 95.1% retention capacity | Warning |
| dead-letter-handler consumer group Dead | Warning |

### Redpanda Cloud (Phase 2)

Configure credentials in the **Data Source** tab of the Settings UI.

Required fields:

| Field | Description |
|---|---|
| **Bootstrap Servers** | From Redpanda Cloud console — Cluster → Overview → Bootstrap servers |
| **SASL Username** | Service account username created in Redpanda Cloud |
| **SASL Password** | Service account password |
| **TLS** | Enable (required for Redpanda Cloud) |

Free tier available at cloud.redpanda.com — no credit card required.

### Apache Kafka / AWS MSK (Phase 3)

Configure in the **Enterprise Kafka** tab once Phase 3 is available. Requires JMX Exporter deployed on each broker, or AWS MSK with CloudWatch metrics enabled.

---

## 8. Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Agent won't start | `DATABASE_URL` not set | Add to `.env` or K8s secret |
| Health check fails | Port mismatch | Verify `PORT=8003` and container exposes port 8003 |
| Sync returns no data | Source type not supported | Use Synthetic Data for Phase 1 |
| AI responses empty | Anthropic key not configured | Add key via the Settings UI |
| Registration fails | `REGISTRY_URL` unreachable | Check platform backend is running. Agent works standalone without `REGISTRY_URL` |
| DB tables not created | ENGINE_URL missing or wrong | Verify `DATABASE_URL` points to the shared postgres and is reachable |
| Lag threshold alerts missing | Threshold too high | Lower `lag_threshold` in Alert Thresholds tab |

### Logs

```bash
# Local Docker
docker compose logs kafka-analyser -f

# Kubernetes
kubectl logs -f deployment/kafka-analyser -n <namespace>
```

### Health check

```bash
curl http://localhost:8003/health
# Expected: {"status": "ok", "agent": "kafka-analyser"}
```

### Generate synthetic data

```bash
curl -X POST http://localhost:8003/reports/generate-sample
# Expected: {"ok": true, "broker_count": 3, "consumer_group_count": 5, ...}
```

### Dashboard overview

```bash
curl http://localhost:8003/dashboard/overview
# Expected: {"cluster": {...}, "brokers": [...], "anomalies": [...]}
```

---

Built by AgentsIQ.
For issues: github.com/agentsiq/kafka-analyser/issues
