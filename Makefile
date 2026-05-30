.PHONY: dev migrate register test logs

# ── Configuration ──────────────────────────────────────────────────────────────
# Load .env so BACKEND_API_KEY, BACKEND_PORT, etc. are available in recipes.
-include .env
export

BACKEND_URL ?= http://localhost:$(or $(BACKEND_PORT),8000)
ALERT_URL   ?= http://localhost:$(or $(ALERT_ANALYSER_PORT),8001)
CUR_URL     ?= http://localhost:$(or $(CUR_ANALYSER_PORT),8002)

# ── Targets ───────────────────────────────────────────────────────────────────

## Start all services (postgres, backend, alert-analyser, cur-analyser, portal)
dev:
	docker compose up --build

## Run Alembic migrations inside the backend container
migrate:
	docker compose run --rm backend alembic upgrade head

## Register and publish both agents into the backend registry (idempotent).
## Prereq: make dev must be running.
register:
	@bash -euc '\
	  _reg() { \
	    slug=$$1 name=$$2 desc=$$3 url=$$4 ver=$$5; \
	    existing=$$(curl -sf $(BACKEND_URL)/api/registry/agents \
	      -H "X-API-Key: $(BACKEND_API_KEY)" \
	      | python3 -c "import sys,json; agents=json.load(sys.stdin); \
	          match=[a[\"id\"] for a in agents if a[\"slug\"]==\"$$slug\"]; \
	          print(match[0] if match else \"\")" 2>/dev/null || true); \
	    if [ -n "$$existing" ]; then \
	      id=$$existing; \
	      echo "[skip] $$slug already registered ($$id)"; \
	    else \
	      id=$$(curl -sf -X POST $(BACKEND_URL)/api/registry/agents \
	        -H "Content-Type: application/json" \
	        -H "X-API-Key: $(BACKEND_API_KEY)" \
	        -d "{\"name\":\"$$name\",\"slug\":\"$$slug\",\"description\":\"$$desc\",\"version\":\"$$ver\",\"invoke_url\":\"$$url\"}" \
	        | python3 -c "import sys,json; print(json.load(sys.stdin)[\"id\"])"); \
	      echo "[ok]   $$slug registered → $$id"; \
	    fi; \
	    curl -sf -X POST $(BACKEND_URL)/api/registry/agents/$$id/publish \
	      -H "X-API-Key: $(BACKEND_API_KEY)" -o /dev/null; \
	    echo "[ok]   $$slug published"; \
	  }; \
	  _reg alert-analyser \
	    "Alert Analyser" \
	    "Analyses OpsGenie alert data, classifies noise, generates suppression recommendations." \
	    "http://alert-analyser:8001" \
	    "0.2.0"; \
	  _reg cur-analyser \
	    "CUR Analyser" \
	    "Analyses AWS Cost and Usage Report data using DuckDB. Cost breakdowns, trends, savings." \
	    "http://cur-analyser:8002" \
	    "0.1.0"'

## Smoke-test invoke on alert-analyser then cur-analyser through the backend.
## Prereq: make dev + make register must be complete.
test:
	@bash -euc '\
	  echo ""; \
	  echo "━━━  Alert Analyser  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
	  ALERTS='"'"'[{"id":"A1","alias":"cpu-high-payment","message":"High CPU on payment-service","source":"payment-service","priority":"P3","acknowledged":false,"status":"closed","count":4,"createdAt":"2024-01-15T10:00:00Z","updatedAt":"2024-01-15T10:02:00Z","report":{"closeTime":120},"teams":["backend"],"integration":{"name":"Datadog"}},{"id":"B1","alias":"db-down","message":"DB primary down","source":"db-primary","priority":"P1","acknowledged":true,"status":"acknowledged","count":1,"createdAt":"2024-01-15T09:00:00Z","updatedAt":"2024-01-15T09:45:00Z","report":{"closeTime":2700},"teams":["platform"],"integration":{"name":"PagerDuty"}}]'"'"'; \
	  PAYLOAD=$$(python3 -c "import json,sys; print(json.dumps({\"session_id\":\"550e8400-e29b-41d4-a716-446655440001\",\"user_message\":\"Classify these alerts and tell me what to suppress.\",\"context\":{\"alerts\":json.loads(sys.argv[1])},\"history\":[]}))" "$$ALERTS"); \
	  curl -sf -X POST $(BACKEND_URL)/api/invoke/alert-analyser \
	    -H "Content-Type: application/json" \
	    -H "X-API-Key: $(BACKEND_API_KEY)" \
	    -d "$$PAYLOAD" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get(\"response\",r))"; \
	  echo ""; \
	  echo "━━━  CUR Analyser  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
	  CSV="line_item_product_code,line_item_unblended_cost,line_item_usage_start_date,line_item_usage_account_id,product_region\nAmazon EC2,210.5,2024-01-01,123456789012,us-east-1\nAmazon RDS,95.2,2024-01-01,123456789012,us-east-1\nAmazon S3,4.32,2024-01-02,123456789012,eu-west-1\nAWS Lambda,1.05,2024-01-02,123456789012,us-east-1\nAmazon EC2,225.0,2024-01-03,123456789012,us-west-2"; \
	  PAYLOAD=$$(python3 -c "import json,sys; csv=sys.argv[1].replace(\"\\\\n\",\"\n\"); print(json.dumps({\"session_id\":\"550e8400-e29b-41d4-a716-446655440002\",\"user_message\":\"What is my total spend and top service? Suggest one saving action.\",\"context\":{\"cur_csv\":csv},\"history\":[]}))" "$$CSV"); \
	  curl -sf -X POST $(BACKEND_URL)/api/invoke/cur-analyser \
	    -H "Content-Type: application/json" \
	    -H "X-API-Key: $(BACKEND_API_KEY)" \
	    -d "$$PAYLOAD" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get(\"response\",r))"'

## Tail logs for all services
logs:
	docker compose logs -f
