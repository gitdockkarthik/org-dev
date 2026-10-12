FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY agents/kafka-analyser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agents/kafka-analyser/ .
COPY shared/ /app/shared/

ENV PYTHONPATH=/app

EXPOSE 8003

# exec: uvicorn replaces the shell as PID 1, so Docker's SIGTERM reaches it and
# the lifespan shutdown runs (a shell as PID 1 ignores SIGTERM; Docker then
# SIGKILLs after the grace period). The shell is kept only to expand PORT.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8003}"]
