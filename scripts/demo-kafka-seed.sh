#!/usr/bin/env bash
#
# demo-kafka-seed.sh — seed the demo Kafka broker (kafka-demo:9095) with
# realistic topics, messages, and consumer-group lag for the Kafka Analyser demo.
#
# Brings up:
#   • 10 topics with realistic names / partition counts
#   • 50 JSON messages per topic
#   • 5 consumer groups that each consume only 10/50 messages (→ 40 lag each)
#
# All Kafka CLI calls run via `docker exec` on the kafka-demo container, so no
# local Kafka tooling is required. Start the stack first, e.g.:
#   docker-compose -f docker-compose.yml -f docker-compose.demo.yml up -d
#
# Usage:
#   bash scripts/demo-kafka-seed.sh
#   KAFKA_CONTAINER=my-kafka bash scripts/demo-kafka-seed.sh   # override container
#
set -uo pipefail

# ── Config ──────────────────────────────────────────────────────────────────
# Confluent 5.3.1 == Apache Kafka 2.3:
#   kafka-topics / kafka-console-consumer → --bootstrap-server
#   kafka-console-producer                → --broker-list (no --bootstrap-server yet)
BROKER="localhost:9095"

# Topic name : partition count
TOPICS=(
  "orders-events:6"
  "payments-processed:3"
  "user-activity:6"
  "inventory-updates:3"
  "notifications:2"
  "fraud-detection:3"
  "audit-log:1"
  "dead-letter-queue:1"
  "schema-changes:1"
  "system-health:2"
)

# Consumer group : topic it consumes (partially — 10 of 50, leaving 40 lag)
GROUPS=(
  "orders-service:orders-events"
  "payments-service:payments-processed"
  "analytics-pipeline:user-activity"
  "fraud-detector:fraud-detection"
  "audit-writer:audit-log"
)

MSGS_PER_TOPIC=50
CONSUME_COUNT=10
TS="2026-06-03T10:00:00Z"

# ── Helpers ─────────────────────────────────────────────────────────────────
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }
die()  { printf '\033[31m✗ %s\033[0m\n' "$1" >&2; exit 1; }

# Run a Kafka CLI tool inside the broker container.
kexec()  { docker exec "$CONTAINER" "$@"; }
# Same, but with stdin attached (for the producer).
kexec_i() { docker exec -i "$CONTAINER" "$@"; }

# ── Resolve the kafka-demo container ────────────────────────────────────────
command -v docker >/dev/null 2>&1 || die "docker not found on PATH"

CONTAINER="${KAFKA_CONTAINER:-$(docker ps --filter "name=kafka-demo" --format '{{.Names}}' | head -n1)}"
[ -n "$CONTAINER" ] || die "No running 'kafka-demo' container found. Start the demo stack first:
    docker-compose -f docker-compose.yml -f docker-compose.demo.yml up -d"

step "Using broker container: $CONTAINER ($BROKER)"

# ── Wait for the broker to accept requests ──────────────────────────────────
step "Waiting for Kafka to be ready…"
ready=""
for _ in $(seq 1 30); do
  if kexec kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
[ -n "$ready" ] || die "Kafka did not become ready in time"
ok "Broker is accepting requests"

# ── 1. Create topics ────────────────────────────────────────────────────────
step "1/3 Creating ${#TOPICS[@]} topics…"
for entry in "${TOPICS[@]}"; do
  topic="${entry%%:*}"
  parts="${entry##*:}"
  if kexec kafka-topics --zookeeper zookeeper-demo:2181 \
        --create \
        --topic "$topic" --partitions "$parts" --replication-factor 1 \
        --if-not-exists \
        >/dev/null 2>&1; then
    ok "$topic ($parts partition(s))"
  else
    die "Failed to create topic $topic"
  fi
done

# ── 2. Produce messages ─────────────────────────────────────────────────────
step "2/3 Producing $MSGS_PER_TOPIC messages to each topic…"
for entry in "${TOPICS[@]}"; do
  topic="${entry%%:*}"
  # Generate the JSON payloads and pipe them into the console producer.
  {
    for i in $(seq 1 "$MSGS_PER_TOPIC"); do
      printf '{"event": "%s", "id": %d, "ts": "%s"}\n' "$topic" "$i" "$TS"
    done
  } | kexec_i kafka-console-producer --broker-list "$BROKER" --topic "$topic" \
      >/dev/null 2>&1 \
      && ok "$topic → $MSGS_PER_TOPIC messages" \
      || die "Failed to produce to $topic"
done

# ── 3. Consume from existing topics to create consumer-group lag ─────────────
# Each group reads only 10 of the 50 messages from its topic, leaving 40 lag.
# Topics already exist (created in step 1) — groups only ever consume, never
# create. Group→topic mapping is fixed and explicit (meaningful group names).
step "3/3 Creating ${#GROUPS[@]} consumer groups (each consuming 10 messages)…"
for entry in "${GROUPS[@]}"; do
  group="${entry%%:*}"
  topic="${entry##*:}"
  # enable.auto.commit ensures offsets are committed when the consumer closes
  # after --max-messages, so the unread messages remain as lag.
  timeout 30 docker exec "$CONTAINER" kafka-console-consumer \
      --bootstrap-server "$BROKER" \
      --group "$group" --topic "$topic" \
      --max-messages "$CONSUME_COUNT" \
      --timeout-ms 10000 \
      --from-beginning \
      --consumer-property enable.auto.commit=true \
      >/dev/null 2>&1 || true
  ok "$group consumed 10 messages"
done

step "Done. Seeded ${#TOPICS[@]} topics and ${#GROUPS[@]} lagging consumer groups."
printf 'Point Kafka Analyser at an Internal Kafka source: bootstrap %s, auth None.\n' "kafka-demo:9095"
