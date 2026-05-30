# Kafka Analyser

Kafka cluster health monitoring, consumer lag detection, broker metrics and anomaly intelligence agent.

---

## 1. Overview

Kafka Analyser is a standalone AI-powered agent that collects Kafka cluster metrics and performs deep health analysis across brokers, consumer groups, topics, and Kafka Connect connectors. It detects consumer lag growth, broker heap pressure, under-replicated partitions, and connector failures using configurable thresholds and Claude-powered reasoning.

**AI capabilities:** cluster health scoring, consumer lag analysis, broker metric interpretation, connector status monitoring, anomaly detection with ranked recommendations.

**Data sources supported:**
- Synthetic data — auto-generated realistic cluster snapshot, always available, no cluster required
- Redpanda Cloud free tier (Phase 2)
- Self-hosted Apache Kafka via JMX Exporter (Phase 3)
- AWS MSK (Phase 3)

**Phase roadmap:**

| Phase | Capability | Status |
|---|---|---|
| 1 | Synthetic cluster data, anomaly detection, 6-tab dashboard, 5-tab Settings UI | Current |
| 2 | Redpanda Cloud live connectivity | UI ready — awaiting credential config |
| 3 | Enterprise Kafka via JMX Exporter and AWS MSK | UI ready — awaiting Phase 2 completion |
| 4 | RAG-grounded analysis using pgvector incident embeddings and Qdrant runbook store | Planned |
| 5 | Autonomous remediation — consumer group restarts, connector restarts, partition scaling | Planned |

---

## 2. Architecture

- **Stateless FastAPI container** — no in-process state; all context injected per request
- **PostgreSQL persistence** — Kafka-specific tables (`kafka_clusters`, `kafka_broker_metrics`, `kafka_consumer_lag`, `kafka_topic_metrics`, `kafka_connector_status`, `kafka_anomalies`) created automatically on first startup
- **Fernet-encrypted config** — Anthropic API key and all cluster credentials stored encrypted in the database under `agent_config`
- **Standalone by default** — no platform dependency; `REGISTRY_URL` is optional and only needed when connecting to the Operative/UAP orchestration layer
- **Claude AI integration** — five tools: `get_cluster_overview`, `get_consumer_lag`, `get_broker_metrics`, `get_topic_metrics`, `detect_anomalies`
- **Settings UI** at `/ui/settings.html` — fully self-contained browser interface, no external frontend required

---

## 3. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL asyncpg connection string |
| `ENCRYPTION_KEY` | Yes | — | Fernet key — encrypts all secrets at rest. Generate once, back up immediately |
| `MODEL` | No | `claude-sonnet-4-6` | Claude model used for inference |
| `PORT` | No | `8003` | HTTP port the container listens on |

> `ANTHROPIC_API_KEY` is intentionally not an env var here — it is entered via the Settings UI and stored encrypted in the database. This allows key rotation without container restarts.

### Generating required secrets

```bash
# ENCRYPTION_KEY
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# POSTGRES_PASSWORD
openssl rand -hex 16
```

⚠️ **CRITICAL — ENCRYPTION_KEY:** Back up this key to AWS Secrets Manager or a password manager immediately after generation. If lost, all encrypted data (including the Anthropic API key) in the database becomes permanently unreadable.

---

## 4. Running Standalone

### Prerequisites

- Docker and Docker Compose installed
- Python 3 and OpenSSL (for secret generation)

### Start

```bash
# 1. Generate secrets
ENCRYPTION_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
POSTGRES_PASSWORD=$(openssl rand -hex 16)

# 2. Create .env
cat > .env <<EOF
ENCRYPTION_KEY=${ENCRYPTION_KEY}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
MODEL=claude-sonnet-4-6
EOF

# 3. Start
docker compose up -d

# 4. Wait for healthy
docker compose ps

# 5. Verify
curl http://localhost:8003/health
# {"status": "ok", "agent": "kafka-analyser"}

# 6. Open Settings UI
open http://localhost:8003/ui/settings.html
```

### Stop

```bash
docker compose down
```

### Rebuild after code changes

```bash
docker compose up -d --build
```

### View logs

```bash
docker compose logs -f kafka-analyser
```

---

## 5. Kubernetes / EKS Deployment

### Build and push image

```bash
docker build -t <ecr-repo>/kafka-analyser:1.0.0 .
docker push <ecr-repo>/kafka-analyser:1.0.0
```

### Create Kubernetes secret

```bash
kubectl create secret generic kafka-analyser-secrets \
  --namespace=<namespace> \
  --from-literal=database-url='postgresql+asyncpg://user:pass@postgres:5432/kafka_db' \
  --from-literal=encryption-key='<your-fernet-key>'
```

### Apply manifests

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl rollout status deployment/kafka-analyser -n <namespace>
```

### Verify

```bash
kubectl get pods -n <namespace>
kubectl logs deployment/kafka-analyser -n <namespace> -f
curl http://<service-ip>:8003/health
```

See [k8s/](k8s/) for the full manifest set.

---

## 6. Post-Deploy Configuration

1. Open the Settings UI at `http://<host>:8003/ui/settings.html`
2. Enter your **Anthropic API key** in the AI Configuration tab — click **Save**
3. Go to the **Data Source** tab — select **Synthetic Data**
4. Click **Save & Sync** — wait for Sync Status to show brokers, consumer groups, and topics loaded
5. Open the **Dashboard** link (or navigate to `/dashboard/overview`) to verify data
6. Chat with the agent by posting to `/invoke` or connecting it to the Operative platform

**For live data (Phase 2 — Redpanda Cloud):**

7. Switch Data Source to **Redpanda Cloud**
8. Enter: Bootstrap Servers, SASL Username, SASL Password, enable TLS
9. Set a Collection Interval (e.g. 5 minutes)
10. Click **Save & Sync** — agent will begin polling the cluster

**Alert thresholds** (Alert Thresholds tab):

| Threshold | Default | Tune when |
|---|---|---|
| Consumer lag critical | 10,000 | Cluster has naturally high-throughput topics |
| Consumer lag warning | 1,000 | Environment has strict SLA |
| Broker heap warning | 75% | JVM tuned for larger heap |
| Under-replicated partitions warning | 1 | Multi-AZ cluster with rolling restarts |

---

## 7. API Reference

All endpoints are unauthenticated in standalone mode. When connected to the Operative platform, the `X-Anthropic-Key` header is injected per-request by the backend orchestrator.

### Core

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check. Returns `{"status": "ok", "agent": "kafka-analyser"}` |
| `POST` | `/invoke` | Chat with the agent. Body: `{session_id, user_message, context, history}` |

### Settings

| Method | Path | Description |
|---|---|---|
| `GET` | `/settings/config` | Get current (decrypted) agent configuration |
| `POST` | `/settings/config` | Save agent configuration (encrypts secrets before storing) |
| `POST` | `/settings/sync` | Trigger a manual data sync from the configured source |
| `GET` | `/settings/sync-status` | Poll sync progress |

### Dashboard

| Method | Path | Description |
|---|---|---|
| `GET` | `/dashboard/overview` | Cluster health score, broker summary, active anomalies |
| `GET` | `/dashboard/brokers` | Per-broker CPU, heap, GC, disk, URP metrics |
| `GET` | `/dashboard/consumer-groups` | Consumer group lag, state, trend per topic/partition |
| `GET` | `/dashboard/topics` | Topic throughput, retention usage, partition health |
| `GET` | `/dashboard/connectors` | Connector state and per-task health |
| `GET` | `/dashboard/anomalies` | Active anomalies with severity, category, recommendations |

### Reports

| Method | Path | Description |
|---|---|---|
| `GET` | `/reports` | List all stored cluster snapshots |
| `GET` | `/reports/{id}` | Get a specific snapshot by ID |
| `GET` | `/reports/{id}/data` | Full cluster data for a snapshot |
| `POST` | `/reports/generate-sample` | Generate a synthetic cluster snapshot and store it |
| `DELETE` | `/reports/{id}` | Delete a snapshot |

### Static UI

| Path | Description |
|---|---|
| `/ui/settings.html` | Settings UI (data source, AI config, thresholds) |

---

## 8. Support & Roadmap

### Phase summary

| Phase | Feature | Notes |
|---|---|---|
| 1 (current) | Synthetic data, anomaly detection, full dashboard and Settings UI | Fully operational |
| 2 | Redpanda Cloud live connectivity | Settings UI complete — activate by entering credentials |
| 3 | Enterprise Kafka via JMX Exporter, AWS MSK with CloudWatch | Settings UI complete — requires JMX sidecar or MSK IAM role |
| 4 | RAG-grounded analysis — pgvector incident history, Qdrant runbook embeddings | Planned — requires vector DB provisioning |
| 5 | Autonomous remediation — consumer group restarts, connector restarts, partition reassignment | Planned — requires Kafka admin credential scope |

### Known limitations (Phase 1)

- Synthetic data only — no live cluster connectivity until Phase 2
- One cluster snapshot active at a time — multiple cluster support planned for Phase 3
- No authentication on API endpoints in standalone mode — deploy behind VPN or add an ingress auth layer

### Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Container won't start | `DATABASE_URL` or `ENCRYPTION_KEY` not set | Add to `.env` and restart |
| `GET /health` returns 502 | Container not yet ready | Wait 10s and retry |
| Sync returns no data | Source type not supported in Phase 1 | Select Synthetic Data |
| AI response empty | Anthropic key not configured | Enter key in Settings UI |
| DB tables missing | Postgres not reachable at startup | Check `docker compose ps` — postgres must be healthy first |
| Cluster data lost on restart | Expected — Phase 1 restores from DB | Verify DB volume is mounted |

### Contact

Built by AgentsIQ. For issues or feature requests, contact the Engineering — Internal Platforms team.
