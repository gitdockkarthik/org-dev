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

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8003}"]
