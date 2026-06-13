"""Prometheus JMX Exporter scraper for Kafka broker metrics.
All HTTP via curl subprocess — fast DNS resolution, handles large payloads.
Per-broker asyncio.wait_for timeout so slow brokers never block others.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

_CURL_MAX_TIME = 50
_BROKER_TIMEOUT = 60.0

_broker_state: dict[str, dict] = {}
_topic_state: dict[str, dict] = {}  # host:port → {topic: {counter: value, time: ts}}

import asyncio as _asyncio
import json as _json

async def _save_scrape_state(key: str, state: dict) -> None:
    """Persist scrape state to AgentConfig for rate continuity across restarts."""
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from datetime import datetime, timezone
        if SessionLocal is None:
            return
        value = _json.dumps(state, default=str)
        now = datetime.now(timezone.utc)
        async with SessionLocal() as session:
            stmt = (
                pg_insert(AgentConfig)
                .values(agent_slug="kafka-analyser", key=key, value=value, updated_at=now)
                .on_conflict_do_update(
                    index_elements=["agent_slug", "key"],
                    set_={"value": value, "updated_at": now},
                )
            )
            await session.execute(stmt)
            await session.commit()
    except Exception:
        pass  # Never break scraping due to state persistence failure


async def _load_scrape_state(key: str) -> dict:
    """Load scrape state from AgentConfig."""
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy import select
        if SessionLocal is None:
            return {}
        async with SessionLocal() as session:
            row = (await session.execute(
                select(AgentConfig).where(
                    AgentConfig.agent_slug == "kafka-analyser",
                    AgentConfig.key == key,
                )
            )).scalar_one_or_none()
        if row:
            return _json.loads(row.value)
        return {}
    except Exception:
        return {}


async def restore_scrape_states() -> None:
    """Restore broker and topic scrape states from DB on startup."""
    global _broker_state, _topic_state
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy import select
        if SessionLocal is None:
            return
        async with SessionLocal() as session:
            rows = (await session.execute(
                select(AgentConfig).where(
                    AgentConfig.agent_slug == "kafka-analyser",
                    AgentConfig.key.like("scrape_state_%"),
                )
            )).scalars().all()
        for row in rows:
            state = _json.loads(row.value)
            key = row.key.replace("scrape_state_", "")
            if ":topic_metrics" in key:
                _topic_state[key] = state
            else:
                _broker_state[key] = state
        logger.info("Restored %d scrape states from DB", len(rows))
    except Exception as exc:
        logger.warning("Failed to restore scrape states: %s", exc)

_FILTERED_METRICS = [
    "jvm_memory_bytes_used",
    "jvm_memory_bytes_max",
    "process_cpu_seconds_total",
    "jvm_gc_collection_seconds_sum",
    "kafka_server_replicamanager_underreplicatedpartitions",
    "kafka_server_replicamanager_atminisrpartitioncount",
    "kafka_server_replicamanager_partitioncount",
    "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
]

_THROUGHPUT_PREFIXES = [
    "kafka_server_brokertopicmetrics_messagesin_total ",
    "kafka_server_brokertopicmetrics_bytesin_total ",
    "kafka_server_brokertopicmetrics_bytesout_total ",
    "kafka_server_replicamanager_isrshrinks_total ",
    "kafka_server_replicamanager_isrexpands_total ",
]


def _parse_prometheus_text(text: str) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+\-]+)", line)
        if not match:
            continue
        name = match.group(1)
        labels_str = match.group(2) or ""
        try:
            value = float(match.group(3))
        except ValueError:
            continue
        labels: dict[str, str] = {}
        if labels_str:
            for lm in re.finditer(r'(\w+)="([^"]*)"', labels_str):
                labels[lm.group(1)] = lm.group(2)
        if name not in result:
            result[name] = []
        result[name].append({"labels": labels, "value": value})
    return result


def _get(metrics: dict, name: str, labels: dict | None = None,
         no_labels_only: bool = False) -> float:
    entries = metrics.get(name, [])
    if not entries:
        return 0.0
    if labels is None and no_labels_only:
        unlabeled = [e for e in entries if not e["labels"]]
        return unlabeled[0]["value"] if unlabeled else 0.0
    if not labels:
        return sum(e["value"] for e in entries)
    for entry in entries:
        if all(entry["labels"].get(k) == v for k, v in labels.items()):
            return entry["value"]
    return 0.0


def _get_topic(metrics: dict, name: str, topic: str) -> float:
    return _get(metrics, name, {"topic": topic})


def _rate(curr: float, prev: float, elapsed: float) -> float:
    if elapsed <= 0 or curr < prev:
        return 0.0
    return round((curr - prev) / elapsed, 2)


async def _curl_get(url: str, max_time: int = 10) -> str:
    """Fetch a URL via curl — fast DNS, no Python HTTP overhead."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "curl", "-s", "--max-time", str(max_time), url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=max_time + 5)
        return stdout.decode(errors="replace") if stdout else ""
    except Exception as exc:
        logging.getLogger(__name__).debug("curl get failed %s: %s", url, exc)
        return ""


async def scrape_broker(host: str, port: int) -> dict[str, Any]:
    """Scrape one broker — Phase 1 filtered curl + Phase 2 curl for throughput."""
    defaults = {
        "heap_pct": 0.0, "cpu_pct": 0.0, "gc_pause_ms": 0.0,
        "messages_in_per_sec": 0.0, "bytes_in_per_sec": 0.0,
        "bytes_out_per_sec": 0.0, "request_handler_idle_pct": 100.0,
        "isr_shrinks_per_sec": 0.0, "isr_expands_per_sec": 0.0,
        "produce_latency_ms": 0.0, "fetch_latency_ms": 0.0,
        "under_replicated_partitions": 0, "at_min_isr_partitions": 0,
        "partition_count": 0,
        "throughput_available": False,
    }
    state_key = f"{host}:{port}"
    try:
        now = time.time()

        # Phase 1: filtered curl — instant, gets JVM/latency/replicamanager metrics
        params = "&".join(f"name[]={m}" for m in _FILTERED_METRICS)
        phase1_raw = await _curl_get(f"http://{host}:{port}/metrics?{params}", max_time=10)
        metrics = _parse_prometheus_text(phase1_raw)

        # Phase 2: full curl — gets throughput counters not available via filter
        phase2_raw = await _curl_get(f"http://{host}:{port}/metrics", max_time=_CURL_MAX_TIME)
        if phase2_raw:
            kept = []
            for line in phase2_raw.splitlines():
                if not line or line.startswith("#"):
                    continue
                # Throughput counters — match by prefix (unlabeled broker aggregates)
                matched = False
                for prefix in _THROUGHPUT_PREFIXES:
                    if line.startswith(prefix):
                        kept.append(line)
                        matched = True
                        break
                # Latency — match by substring (label order may vary)
                if not matched and "requestmetrics_totaltimems" in line:
                    if 'quantile="0.999"' in line and (
                        'request="Produce"' in line or 'request="Fetch"' in line
                    ):
                        kept.append(line)
            if kept:
                metrics.update(_parse_prometheus_text("\n".join(kept)))

        throughput_available = bool(phase2_raw)

        prev = _broker_state.get(state_key, {})
        prev_metrics = prev.get("metrics", {})
        prev_time = prev.get("time", now)
        elapsed = now - prev_time if prev_time != now else 60.0

        heap_used = _get(metrics, "jvm_memory_bytes_used", {"area": "heap"})
        heap_max = _get(metrics, "jvm_memory_bytes_max", {"area": "heap"})
        heap_pct = round((heap_used / heap_max) * 100, 1) if heap_max > 0 else 0.0

        cpu_curr = _get(metrics, "process_cpu_seconds_total")
        cpu_prev = _get(prev_metrics, "process_cpu_seconds_total") if prev_metrics else cpu_curr
        cpu_pct = min(100.0, max(0.0, round(_rate(cpu_curr, cpu_prev, elapsed) * 100, 1)))

        gc_curr = _get(metrics, "jvm_gc_collection_seconds_sum", {"gc": "G1 Young Generation"})
        gc_prev = _get(prev_metrics, "jvm_gc_collection_seconds_sum",
                      {"gc": "G1 Young Generation"}) if prev_metrics else gc_curr
        gc_ms = round(_rate(gc_curr, gc_prev, elapsed) * 1000, 1)

        def rate_metric(name: str) -> float:
            curr = _get(metrics, name, no_labels_only=True)
            prev_val = _get(prev_metrics, name, no_labels_only=True) if prev_metrics else curr
            return _rate(curr, prev_val, elapsed)

        msgs_in = rate_metric("kafka_server_brokertopicmetrics_messagesin_total")
        bytes_in = rate_metric("kafka_server_brokertopicmetrics_bytesin_total")
        bytes_out = rate_metric("kafka_server_brokertopicmetrics_bytesout_total")
        isr_shrinks = rate_metric("kafka_server_replicamanager_isrshrinks_total")
        isr_expands = rate_metric("kafka_server_replicamanager_isrexpands_total")

        produce_latency = _get(metrics, "kafka_network_requestmetrics_totaltimems",
                               {"quantile": "0.999", "request": "Produce"})
        fetch_latency = _get(metrics, "kafka_network_requestmetrics_totaltimems",
                             {"quantile": "0.999", "request": "Fetch"})

        urp = int(_get(metrics, "kafka_server_replicamanager_underreplicatedpartitions"))
        at_min_isr = int(_get(metrics, "kafka_server_replicamanager_atminisrpartitioncount"))
        partition_count = int(_get(metrics, "kafka_server_replicamanager_partitioncount"))
        req_idle = _get(metrics,
                       "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent")
        req_idle_pct = (req_idle if
                       "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent"
                       in metrics else 100.0)

        _broker_state[state_key] = {"metrics": metrics, "time": now}
        _asyncio.ensure_future(_save_scrape_state(
            f"scrape_state_{state_key}",
            {"metrics": metrics, "time": now}
        ))

        return {
            "heap_pct": heap_pct, "cpu_pct": cpu_pct, "gc_pause_ms": gc_ms,
            "messages_in_per_sec": msgs_in, "bytes_in_per_sec": bytes_in,
            "bytes_out_per_sec": bytes_out, "request_handler_idle_pct": req_idle_pct,
            "isr_shrinks_per_sec": isr_shrinks, "isr_expands_per_sec": isr_expands,
            "produce_latency_ms": produce_latency, "fetch_latency_ms": fetch_latency,
            "under_replicated_partitions": urp, "at_min_isr_partitions": at_min_isr,
            "partition_count": partition_count,
            "throughput_available": throughput_available,
        }
    except Exception as exc:
        logger.warning("Prometheus scrape failed for %s:%s: %s", host, port, exc)
        return defaults


async def scrape_all_brokers(brokers: list[dict], prometheus_port: int,
                              per_broker_timeout: float = _BROKER_TIMEOUT) -> dict[str, dict]:
    """Scrape all brokers in parallel — each with independent timeout."""
    async def scrape_with_timeout(host: str, broker_id: str):
        try:
            result = await asyncio.wait_for(
                scrape_broker(host, prometheus_port),
                timeout=per_broker_timeout
            )
            return broker_id, result
        except asyncio.TimeoutError:
            logger.warning("Broker %s timed out (>%ss) — metrics unavailable",
                          broker_id, per_broker_timeout)
            return broker_id, {"metrics_unavailable": True}
        except Exception as exc:
            logger.warning("Broker %s scrape failed: %s", broker_id, exc)
            return broker_id, {"metrics_unavailable": True}

    tasks = []
    for broker in brokers:
        host = broker.get("host", "")
        if not host:
            continue
        broker_id = str(broker.get("broker_id", host))
        tasks.append(scrape_with_timeout(host, broker_id))

    results = {}
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    for item in completed:
        if isinstance(item, Exception):
            logger.warning("Unexpected scrape error: %s", item)
        else:
            broker_id, data = item
            results[broker_id] = data
    return results


async def scrape_topic_metrics(host: str, prometheus_port: int,
                                topic_names: list[str]) -> dict[str, dict]:
    """Scrape per-topic metrics via curl."""
    result: dict[str, dict] = {}
    if not topic_names:
        return result
    topic_set = set(topic_names)
    try:
        raw = await _curl_get(f"http://{host}:{prometheus_port}/metrics",
                              max_time=_CURL_MAX_TIME)
        if not raw:
            return result

        kept = []
        for line in raw.splitlines():
            if not line or line.startswith("#"):
                continue
            if ("kafka_server_brokertopicmetrics" not in line
                    and "kafka_log_log_size" not in line):
                continue
            topic_match = re.search(r'topic="([^"]*)"', line)
            if topic_match and topic_match.group(1) in topic_set:
                kept.append(line)

        metrics = _parse_prometheus_text("\n".join(kept))

        topic_sizes: dict[str, float] = {}
        for entry in metrics.get("kafka_log_log_size", []):
            topic = entry["labels"].get("topic", "")
            if topic:
                topic_sizes[topic] = topic_sizes.get(topic, 0) + entry["value"]

        now = time.time()
        state_key = f"{host}:{prometheus_port}:topic_metrics"
        prev_state = _topic_state.get(state_key, {})
        prev_time = prev_state.get("__time__", now)
        elapsed = now - prev_time if now != prev_time else 60.0

        new_state: dict = {"__time__": now}
        for topic in topic_names:
            msgs_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_messagesin_by_topic_total", topic)
            bytes_in_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_bytesin_by_topic_total", topic)
            bytes_out_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_bytesout_by_topic_total", topic)

            msgs_prev = prev_state.get(f"{topic}__msgs", msgs_curr)
            bytes_in_prev = prev_state.get(f"{topic}__bytes_in", bytes_in_curr)
            bytes_out_prev = prev_state.get(f"{topic}__bytes_out", bytes_out_curr)

            new_state[f"{topic}__msgs"] = msgs_curr
            new_state[f"{topic}__bytes_in"] = bytes_in_curr
            new_state[f"{topic}__bytes_out"] = bytes_out_curr

            result[topic] = {
                "messages_in_per_sec": _rate(msgs_curr, msgs_prev, elapsed),
                "bytes_in_per_sec": _rate(bytes_in_curr, bytes_in_prev, elapsed),
                "bytes_out_per_sec": _rate(bytes_out_curr, bytes_out_prev, elapsed),
                "size_bytes": int(topic_sizes.get(topic, 0)),
            }

        _topic_state[state_key] = new_state
        _asyncio.ensure_future(_save_scrape_state(
            f"scrape_state_{state_key}",
            new_state
        ))
    except Exception as exc:
        logger.warning("Topic metrics scrape failed for %s:%s: %s",
                      host, prometheus_port, exc)
    return result


async def get_top_topics_by_log_size(host: str, prometheus_port: int,
                                      top_n: int = 20) -> list[dict]:
    """Get top N topics by log size from ALL topics in the cluster."""
    try:
        raw = await _curl_get(f"http://{host}:{prometheus_port}/metrics",
                              max_time=_CURL_MAX_TIME)
        if not raw:
            return []

        topic_sizes: dict[str, float] = {}
        for line in raw.splitlines():
            if not line.startswith("kafka_log_log_size{"):
                continue
            topic_match = re.search(r'topic="([^"]*)"', line)
            val_match = re.search(r'[\},]\s*([\d.eE+\-]+)\s*$', line)
            if topic_match and val_match:
                topic = topic_match.group(1)
                try:
                    size = float(val_match.group(1))
                    topic_sizes[topic] = topic_sizes.get(topic, 0) + size
                except ValueError:
                    pass

        sorted_topics = sorted(topic_sizes.items(), key=lambda x: x[1], reverse=True)
        return [{"name": t, "size_bytes": int(s)} for t, s in sorted_topics[:top_n]]
    except Exception as exc:
        logger.warning("Top topics by log size failed for %s:%s: %s",
                      host, prometheus_port, exc)
        return []


async def scrape_topic_metrics_and_top_by_size(
        host: str, prometheus_port: int,
        topic_names: list[str],
        top_n: int = 20) -> tuple[dict[str, dict], list[dict]]:
    """Single curl fetch — returns both per-topic metrics AND top N topics by log size.
    Avoids double fetch that scrape_topic_metrics + get_top_topics_by_log_size would cause."""
    result: dict[str, dict] = {}
    top_by_size: list[dict] = []
    if not topic_names:
        return result, top_by_size
    topic_set = set(topic_names)
    try:
        raw = await _curl_get(f"http://{host}:{prometheus_port}/metrics",
                              max_time=_CURL_MAX_TIME)
        if not raw:
            return result, top_by_size

        kept = []
        all_sizes: dict[str, float] = {}

        for line in raw.splitlines():
            if not line or line.startswith("#"):
                continue
            # Collect ALL log sizes for top-by-size ranking
            if line.startswith("kafka_log_log_size{"):
                topic_match = re.search(r'topic="([^"]*)"', line)
                val_match = re.search(r'[\},]\s*([\d.eE+\-]+)\s*$', line)
                if topic_match and val_match:
                    topic = topic_match.group(1)
                    try:
                        all_sizes[topic] = all_sizes.get(topic, 0) + float(val_match.group(1))
                    except ValueError:
                        pass
                # Also keep for per-topic metrics if in topic_set
                if topic_match and topic_match.group(1) in topic_set:
                    kept.append(line)
                continue
            # Per-topic throughput metrics for requested topics only
            if "kafka_server_brokertopicmetrics" in line:
                topic_match = re.search(r'topic="([^"]*)"', line)
                if topic_match and topic_match.group(1) in topic_set:
                    kept.append(line)

        # Build top N by size from all topics
        sorted_sizes = sorted(all_sizes.items(), key=lambda x: x[1], reverse=True)
        top_by_size = [{"name": t, "size_bytes": int(s)} for t, s in sorted_sizes[:top_n]]

        # Parse per-topic metrics
        metrics = _parse_prometheus_text("\n".join(kept))

        topic_sizes: dict[str, float] = {}
        for entry in metrics.get("kafka_log_log_size", []):
            topic = entry["labels"].get("topic", "")
            if topic:
                topic_sizes[topic] = topic_sizes.get(topic, 0) + entry["value"]

        now = time.time()
        state_key = f"{host}:{prometheus_port}:topic_metrics_top"
        prev_state = _topic_state.get(state_key, {})
        prev_time = prev_state.get("__time__", now)
        elapsed = now - prev_time if now != prev_time else 60.0

        new_state: dict = {"__time__": now}
        for topic in topic_names:
            msgs_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_messagesin_by_topic_total", topic)
            bytes_in_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_bytesin_by_topic_total", topic)
            bytes_out_curr = _get_topic(metrics,
                "kafka_server_brokertopicmetrics_bytesout_by_topic_total", topic)

            msgs_prev = prev_state.get(f"{topic}__msgs", msgs_curr)
            bytes_in_prev = prev_state.get(f"{topic}__bytes_in", bytes_in_curr)
            bytes_out_prev = prev_state.get(f"{topic}__bytes_out", bytes_out_curr)

            new_state[f"{topic}__msgs"] = msgs_curr
            new_state[f"{topic}__bytes_in"] = bytes_in_curr
            new_state[f"{topic}__bytes_out"] = bytes_out_curr

            result[topic] = {
                "messages_in_per_sec": _rate(msgs_curr, msgs_prev, elapsed),
                "bytes_in_per_sec": _rate(bytes_in_curr, bytes_in_prev, elapsed),
                "bytes_out_per_sec": _rate(bytes_out_curr, bytes_out_prev, elapsed),
                "size_bytes": int(topic_sizes.get(topic, 0)),
            }

        _topic_state[state_key] = new_state
        _asyncio.ensure_future(_save_scrape_state(
            f"scrape_state_{state_key}",
            new_state
        ))
    except Exception as exc:
        logger.warning("Topic metrics scrape failed for %s:%s: %s",
                      host, prometheus_port, exc)
    return result, top_by_size
