FROM python:3.11-slim

WORKDIR /app

COPY agents/alert-analyser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agents/alert-analyser/ .
COPY shared/ /app/shared/

ENV PYTHONPATH=/app

EXPOSE 8001

# PORT is injected by Railway; falls back to 8001 for local docker-compose
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8001}"]
