# Operative Intelligence — Dev Platform

Internal AI agent platform for engineering and operations teams.

## Overview

Operative is a self-hosted AI agent platform that runs on a single Docker box inside your VPN. It provides a browser-based portal where engineering and operations teams can interact with specialised AI agents, view dashboards, and configure data sources. Each agent is a standalone FastAPI service that connects to the Anthropic API and exposes a consistent chat and settings interface. Two agents are included in this release: **Alert Analyser** (OpsGenie/JSM noise detection and triage) and **CUR Analyser** (AWS Cost and Usage Report analysis). The platform uses a plugin model — new agents are added by cloning an agent repo and adding one service block to `docker-compose.yml`. Access is via browser over VPN; no public endpoints are exposed.

## Prerequisites

### Windows Jump Box
- VS Code installed
- Git installed
- Browser (Chrome/Edge)

### AL2023 Docker Box

Install Docker and Docker Compose:
```bash
sudo dnf update -y
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
# Log out and back in for group changes to take effect
```

Install Python3 and OpenSSL (required by setup script):
```bash
sudo dnf install -y python3 openssl
sudo pip3 install cryptography
```

Install Git:
```bash
sudo dnf install -y git
```

## First Time Setup

### Step 1 — Clone the repository
```bash
git clone https://github.com/gitdockkarthik/org-dev.git
cd org-dev
```

### Step 2 — Run setup script
```bash
./setup-env.sh
```

The script will:
- Generate ENCRYPTION_KEY, POSTGRES_PASSWORD, SECRET_KEY and BACKEND_API_KEY automatically
- Create .env with secure permissions (600)
- Display your ENCRYPTION_KEY prominently

⚠️ CRITICAL: Back up your ENCRYPTION_KEY immediately.
This key encrypts all secrets in PostgreSQL.
If lost, encrypted data cannot be recovered.
Store it in:
- AWS Secrets Manager (recommended)
- Password manager
- Secure note

### Step 3 — Start the platform
```bash
docker compose up -d postgres backend portal
```

Wait for all three services to be healthy:
```bash
docker compose ps
```

All three should show STATUS: Up or healthy.

### Step 4 — Complete setup wizard

Open in browser from Windows jump box:
```
http://<AL2023-private-ip>:3000/setup.html
```

The wizard has 3 steps:
1. Enter your Anthropic API key → Test Connection → Next
2. Copy the generated Platform API key (save it — shown only once) → Save & Continue
3. Done — click Launch

### Step 5 — Start agents
```bash
docker compose up -d alert-analyser cur-analyser
```

Wait 30 seconds then refresh the browser.
Both agents should appear as PUBLISHED.

### Step 6 — Configure agents via Settings UI

For each agent click Settings in the agent card:

**Alert Analyser:**
1. Enter Anthropic API key if not inherited
2. Select data source:
   - Synthetic Data (recommended for first test)
   - OpsGenie / JSM API (enter credentials)
   - File Upload (upload CUR CSV)
3. Click Save & Sync
4. Verify Sync Status shows data loaded

**CUR Analyser:**
1. Select data source:
   - Synthetic Data (recommended for first test)
   - File Upload (upload AWS CUR CSV)
2. Click Save & Sync
3. Verify Sync Status shows records loaded

## DNS Setup (when available)

Once InfraOps creates the DNS record:
```
http://ai-dev.internal.yourorg.com
```
No changes needed — just use the URL instead of IP.

## Adding a New Agent

When a new agent is ready (e.g. kafka-analyser):

1. Add agent folder:
```bash
git clone https://github.com/agentsiq/<agent-name>.git \
  agents/<agent-name>
```

2. Add service to docker-compose.yml:
```yaml
  kafka-analyser:
    build:
      context: ./agents/kafka-analyser
    ports:
      - "8003:8080"
    environment:
      REGISTRY_URL: http://backend:8000
      BACKEND_API_KEY: ${BACKEND_API_KEY}
    depends_on:
      - backend
```

3. Start the agent:
```bash
docker compose up -d kafka-analyser
```

Agent appears in portal automatically.
Configure via Settings UI.

## Daily Operations

### Start everything
```bash
docker compose up -d
```

### Stop everything
```bash
docker compose down
```

### View logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f alert-analyser
```

### Restart a single agent
```bash
docker compose restart alert-analyser
```

### Check all service status
```bash
docker compose ps
```

## Troubleshooting

| Issue | Likely cause | Fix |
|---|---|---|
| Backend crashes on startup | ENCRYPTION_KEY mismatch with DB | Run: docker compose down -v then setup-env.sh again |
| Portal shows "Could not load agents" | Backend not running | Check: docker compose ps and logs |
| Agent not appearing in portal | Agent not started | Run: docker compose up -d \<agent-name\> |
| Setup wizard shows 502 error | Backend not ready | Wait 30 seconds and retry |
| Cannot reach portal | Wrong IP or Docker not running | Check: docker compose ps |
| Agent settings not saving | Anthropic key not configured | Enter key in Settings UI first |

## Security Notes

- .env file has 600 permissions — readable only by owner
- All API keys stored encrypted in PostgreSQL
- ENCRYPTION_KEY must be backed up externally
- PostgreSQL port (5432) not exposed outside Docker network
- Access via VPN only — no public endpoints

## Secret Management Roadmap

| Secret | Dev (now) | EKS (future) |
|---|---|---|
| ENCRYPTION_KEY | .env (chmod 600) | AWS Secrets Manager |
| POSTGRES_PASSWORD | .env (chmod 600) | AWS Secrets Manager |
| Anthropic API key | Setup wizard → DB | Setup wizard → DB |
| Agent credentials | Settings UI → DB | Settings UI → DB |

## Platform URLs

| Service | URL | Notes |
|---|---|---|
| Portal | http://\<ip\>:3000 | Main access point |
| Setup wizard | http://\<ip\>:3000/setup.html | First run only |
| Backend API | http://\<ip\>:8000 | Internal only |
| Alert Analyser | http://\<ip\>:8001 | Internal only |
| CUR Analyser | http://\<ip\>:8002 | Internal only |

---
Internal use only. Engineering — Internal Platforms.
