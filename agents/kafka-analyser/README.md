# Kafka Analyser
AI-powered Kafka cluster intelligence agent — multi-cluster monitoring, 
consumer lag detection, broker health, and Claude-powered insights.

## 1. Overview
Kafka Analyser connects to real Kafka clusters and provides:
- Multi-cluster support — monitor dev, nonprod, and prod clusters simultaneously
- Real-time auto-collection — configurable sync interval per deployment
- Consumer lag analysis with trend detection and ETA to breach
- Broker health monitoring (heap, CPU, URP, GC)
- Topic metrics (throughput, retention, partition health)
- Schema Registry monitoring (Phase 2)
- AI-powered insights — Claude-generated cluster health narrative and recommendations
- Configurable anomaly detection with actionable recommendations
- Chatbot — conversational Kafka intelligence

## 2. Current Phase Status
- Phase 1 ✅ LIVE — Synthetic data, anomaly detection, dashboard, Settings UI
- Phase 2 ✅ LIVE — Real Kafka clusters (Apache, Confluent 5.x+, Redpanda), multi-cluster registry, cluster selector dashboard, auto-collection loop
- Phase 3 🔧 Planned — Schema Registry tab, ZooKeeper metrics, Kafka Connect real data
- Phase 4 🔧 Planned — MirrorMaker detection, JMX metrics for self-hosted brokers
- Phase 5 🔧 Planned — RAG-grounded analysis, pgvector incident embeddings
- Phase 6 🔧 Planned — Autonomous remediation

## 3. Architecture
Stateless FastAPI agent. Connects to Kafka brokers via Admin API (kafka-python-ng).
Supports all Kafka distributions: Apache Kafka, Confluent Platform (5.x+), 
Redpanda, AWS MSK.

Auth support:
- None (internal brokers)
- SASL/PLAIN
- SASL/SCRAM-SHA-256
- SASL/SCRAM-SHA-512
- SASL/GSSAPI (Kerberos) — config only, Phase 3
- mTLS — config only, Phase 3

## 4. Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| DATABASE_URL | Yes* | — | PostgreSQL connection string |
| ENCRYPTION_KEY | Yes | — | Fernet key for secret encryption |
| STORAGE_BACKEND | No | postgres | postgres or memory |
| REGISTRY_URL | No | — | Platform backend URL for self-registration |
| BACKEND_API_KEY | No | — | Platform API key |
| MODEL | No | claude-sonnet-4-6 | Claude model for AI insights |
| AGENT_SLUG | No | kafka-analyser | Agent identifier |

*Not required when STORAGE_BACKEND=memory

## 5. Docker — Dev Setup

### Standalone (memory mode — no DB needed)
```bash
docker run -p 8003:8003 \
  -e STORAGE_BACKEND=memory \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  operative/kafka-analyser:2.0.0
```

### With PostgreSQL
```bash
docker run -p 8003:8003 \
  -e DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/db \
  -e ENCRYPTION_KEY=<fernet-key> \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  operative/kafka-analyser:2.0.0
```

## 6. Kubernetes (EKS) Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-analyser
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: kafka-analyser
        image: <ECR>/kafka-analyser:2.0.0
        ports:
        - containerPort: 8003
        env:
        - name: STORAGE_BACKEND
          value: postgres
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: platform-db
              key: url
        - name: ENCRYPTION_KEY
          valueFrom:
            secretKeyRef:
              name: platform-secrets
              key: encryption-key
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: platform-secrets
              key: anthropic-key
        livenessProbe:
          httpGet:
            path: /health
            port: 8003
        readinessProbe:
          httpGet:
            path: /health
            port: 8003
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
```

## 7. API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| /health | GET | Health check |
| /invoke | POST | AI chatbot |
| /settings | GET/POST | Agent configuration |
| /settings/clusters | GET/POST | Cluster registry |
| /settings/clusters/{id} | PUT/DELETE | Manage cluster |
| /settings/clusters/{id}/test | POST | Test connection |
| /settings/clusters/{id}/enable | POST | Enable/disable |
| /settings/sync | POST | Trigger manual sync |
| /dashboard/overview | GET | Cluster health summary |
| /dashboard/consumer-groups | GET | Consumer lag data |
| /dashboard/topics | GET | Topic metrics |
| /dashboard/brokers | GET | Broker metrics |
| /dashboard/connectors | GET | Connector status |
| /dashboard/insights | GET | Active anomalies |
| /dashboard/insights/narrative | POST | AI cluster analysis |
| /reports | GET | Sync history |

All dashboard endpoints accept ?cluster_id= query parameter.

## 8. Integration Models

### Model A — iframe embed
```html
<iframe src="http://kafka-analyser:8003/ui/dashboard.html" 
        width="100%" height="100%" />
```

### Model B — AI chat API
```bash
curl -X POST http://kafka-analyser:8003/invoke \
  -H "Content-Type: application/json" \
  -H "X-Anthropic-Key: sk-ant-..." \
  -d '{"session_id":"abc","user_message":"What is the consumer lag?"}'
```

### Model C — Dashboard data API
```bash
curl http://kafka-analyser:8003/dashboard/overview?cluster_id=1
curl http://kafka-analyser:8003/dashboard/consumer-groups?cluster_id=1
```

## 9. Configuring Kafka Clusters
Clusters are configured via the Settings UI or API — no file editing required.

Add a cluster via API:
```bash
curl -X POST http://kafka-analyser:8003/settings/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production",
    "environment": "prod",
    "bootstrap_servers": "broker1:9092,broker2:9092",
    "auth_type": "none"
  }'
```

## 10. Health Check
```bash
curl http://kafka-analyser:8003/health
# {"status":"ok","agent":"kafka-analyser"}
```

## 11. Changelog
See CHANGELOG.md
