"""Prometheus JMX Exporter scraper for Kafka broker metrics.
Scrapes broker JMX Exporter HTTP endpoint directly — no RMI, no jpype, no Prometheus server.
Uses streaming line-by-line filter to extract only needed metrics from large response.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Timeout for full /metrics dump — large clusters need up to 120s
_SCRAPE_TIMEOUT = 60.0

# Per-broker persistent state for rate calculations (keyed by host:port)
_broker_state: dict[str, dict] = {}

# Metrics we extract from broker scrape
_BROKER_WANTED = {
    "jvm_memory_bytes_used",
    "jvm_memory_bytes_max",
    "process_cpu_seconds_total",
    "jvm_gc_collection_seconds_sum",
    "kafka_server_brokertopicmetrics_messagesin_total",
    "kafka_server_brokertopicmetrics_bytesin_total",
    "kafka_server_brokertopicmetrics_bytesout_total",
    "kafka_server_replicamanager_underreplicatedpartitions",
    "kafka_server_replicamanager_atminisrpartitioncount",
    "kafka_server_replicamanager_partitioncount",
    "kafka_server_replicamanager_isrshrinks_total",
    "kafka_server_replicamanager_isrexpands_total",
    "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
    "kafka_network_requestmetrics_totaltimems",
}

# Topic metrics we extract
_TOPIC_WANTED = {
    "kafka_server_brokertopicmetrics_messagesin_by_topic_total",
    "kafka_server_brokertopicmetrics_bytesin_by_topic_total",
    "kafka_server_brokertopicmetrics_bytesout_by_topic_total",
    "kafka_log_log_size",
}


def _parse_prometheus_text(text: str) -> dict[str, list[dict]]:
    """Parse Prometheus text format into {metric_name: [{labels, value}]}."""
    result: dict[str, list[dict]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        match = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+\-]+)', line)
        if not match:
            continue
        name = match.group(1)
        labels_str = match.group(2) or ''
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
        result[name].append({'labels': labels, 'value': value})
    return result


def _get(metrics: dict, name: str, labels: dict | None = None,
         no_labels_only: bool = False) -> float:
    """Get metric value by name and optional label filters."""
    entries = metrics.get(name, [])
    if not entries:
        return 0.0
    if labels is None and no_labels_only:
        unlabeled = [e for e in entries if not e['labels']]
        return unlabeled[0]['value'] if unlabeled else 0.0
    if not labels:
        return sum(e['value'] for e in entries)
    for entry in entries:
        if all(entry['labels'].get(k) == v for k, v in labels.items()):
            return entry['value']
    return 0.0


def _get_topic(metrics: dict, name: str, topic: str) -> float:
    return _get(metrics, name, {'topic': topic})


def _rate(curr: float, prev: float, elapsed: float) -> float:
    if elapsed <= 0 or curr < prev:
        return 0.0
    return round((curr - prev) / elapsed, 2)


async def _stream_filtered_lines(host: str, port: int,
                                  wanted: set[str],
                                  skip_topic_labels: bool = False) -> str:
    """Stream /metrics and return only lines matching wanted metric names."""
    kept: list[str] = []
    async with httpx.AsyncClient(timeout=_SCRAPE_TIMEOUT) as client:
        async with client.stream("GET", f"http://{host}:{port}/metrics") as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line or line.startswith('#'):
                    continue
                # Extract metric name
                name_end = len(line)
                for i, ch in enumerate(line):
                    if ch in ('{', ' '):
                        name_end = i
                        break
                metric_name = line[:name_end]
                if metric_name not in wanted:
                    continue
                # For broker scrape: skip per-topic labeled entries for throughput counters
                if skip_topic_labels and 'topic=' in line and 'brokertopicmetrics' in metric_name:
                    continue
                kept.append(line)
    return "\n".join(kept)


async def scrape_broker(host: str, port: int) -> dict[str, Any]:
    """Scrape broker metrics — streams /metrics, extracts only needed lines."""
    defaults = {
        "heap_pct": 0.0, "cpu_pct": 0.0, "gc_pause_ms": 0.0,
        "messages_in_per_sec": 0.0, "bytes_in_per_sec": 0.0,
        "bytes_out_per_sec": 0.0, "request_handler_idle_pct": 100.0,
        "isr_shrinks_per_sec": 0.0, "isr_expands_per_sec": 0.0,
        "produce_latency_ms": 0.0, "fetch_latency_ms": 0.0,
        "under_replicated_partitions": 0, "at_min_isr_partitions": 0,
        "partition_count": 0,
    }
    state_key = f"{host}:{port}"
    try:
        now = time.time()
        text = await _stream_filtered_lines(host, port, _BROKER_WANTED, skip_topic_labels=True)
        metrics = _parse_prometheus_text(text)

        prev = _broker_state.get(state_key, {})
        prev_metrics = prev.get('metrics', {})
        prev_time = prev.get('time', now)
        elapsed = now - prev_time if prev_time != now else 60.0

        # Heap %
        heap_used = _get(metrics, 'jvm_memory_bytes_used', {'area': 'heap'})
        heap_max = _get(metrics, 'jvm_memory_bytes_max', {'area': 'heap'})
        heap_pct = round((heap_used / heap_max) * 100, 1) if heap_max > 0 else 0.0

        # CPU % rate
        cpu_curr = _get(metrics, 'process_cpu_seconds_total')
        cpu_prev = _get(prev_metrics, 'process_cpu_seconds_total') if prev_metrics else cpu_curr
        cpu_pct = min(100.0, max(0.0, round(_rate(cpu_curr, cpu_prev, elapsed) * 100, 1)))

        # GC pause rate
        gc_curr = _get(metrics, 'jvm_gc_collection_seconds_sum', {'gc': 'G1 Young Generation'})
        gc_prev = _get(prev_metrics, 'jvm_gc_collection_seconds_sum',
                      {'gc': 'G1 Young Generation'}) if prev_metrics else gc_curr
        gc_ms = round(_rate(gc_curr, gc_prev, elapsed) * 1000, 1)

        # Throughput rates — broker aggregate (no topic label)
        def rate_metric(name: str) -> float:
            curr = _get(metrics, name, no_labels_only=True)
            prev_val = _get(prev_metrics, name, no_labels_only=True) if prev_metrics else curr
            return _rate(curr, prev_val, elapsed)

        msgs_in = rate_metric('kafka_server_brokertopicmetrics_messagesin_total')
        bytes_in = rate_metric('kafka_server_brokertopicmetrics_bytesin_total')
        bytes_out = rate_metric('kafka_server_brokertopicmetrics_bytesout_total')
        isr_shrinks = rate_metric('kafka_server_replicamanager_isrshrinks_total')
        isr_expands = rate_metric('kafka_server_replicamanager_isrexpands_total')

        # Latency
        produce_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                              {'quantile': '0.999', 'request': 'Produce'})
        fetch_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                            {'quantile': '0.999', 'request': 'Fetch'})

        # Gauges
        urp = int(_get(metrics, 'kafka_server_replicamanager_underreplicatedpartitions'))
        at_min_isr = int(_get(metrics, 'kafka_server_replicamanager_atminisrpartitioncount'))
        partition_count = int(_get(metrics, 'kafka_server_replicamanager_partitioncount'))
        req_idle = _get(metrics, 'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent')
        req_idle_pct = req_idle if 'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent' in metrics else 100.0

        # Store state for next rate calculation
        _broker_state[state_key] = {'metrics': metrics, 'time': now}

        return {
            "heap_pct": heap_pct, "cpu_pct": cpu_pct, "gc_pause_ms": gc_ms,
            "messages_in_per_sec": msgs_in, "bytes_in_per_sec": bytes_in,
            "bytes_out_per_sec": bytes_out, "request_handler_idle_pct": req_idle_pct,
            "isr_shrinks_per_sec": isr_shrinks, "isr_expands_per_sec": isr_expands,
            "produce_latency_ms": produce_latency, "fetch_latency_ms": fetch_latency,
            "under_replicated_partitions": urp, "at_min_isr_partitions": at_min_isr,
            "partition_count": partition_count,
        }
    except Exception as exc:
        logger.warning("Prometheus scrape failed for %s:%s: %s", host, port, exc)
        return defaults


async def scrape_all_brokers(brokers: list[dict], prometheus_port: int,
                              per_broker_timeout: float = 90.0) -> dict[str, dict]:
    """Scrape all brokers in parallel — each broker has independent timeout."""
    async def scrape_with_timeout(host: str, broker_id: str):
        try:
            result = await asyncio.wait_for(
                scrape_broker(host, prometheus_port),
                timeout=per_broker_timeout
            )
            return broker_id, result
        except asyncio.TimeoutError:
            logger.warning("Broker scrape timed out for %s (>%ss)", broker_id, per_broker_timeout)
            return broker_id, {"metrics_unavailable": True}
        except Exception as exc:
            logger.warning("Broker scrape failed for %s: %s", broker_id, exc)
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
    """Scrape per-topic metrics — streams and extracts only requested topics."""
    result: dict[str, dict] = {}
    if not topic_names:
        return result
    topic_set = set(topic_names)
    try:
        kept: list[str] = []
        async with httpx.AsyncClient(timeout=_SCRAPE_TIMEOUT) as client:
            async with client.stream("GET", f"http://{host}:{prometheus_port}/metrics") as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line or line.startswith('#'):
                        continue
                    # Check if line is a topic metric we want
                    is_topic_metric = any(m in line for m in _TOPIC_WANTED)
                    if not is_topic_metric:
                        continue
                    topic_match = re.search(r'topic="([^"]*)"', line)
                    if topic_match and topic_match.group(1) in topic_set:
                        kept.append(line)

        metrics = _parse_prometheus_text("\n".join(kept))

        topic_sizes: dict[str, float] = {}
        for entry in metrics.get('kafka_log_log_size', []):
            topic = entry['labels'].get('topic', '')
            if topic:
                topic_sizes[topic] = topic_sizes.get(topic, 0) + entry['value']

        for topic in topic_names:
            result[topic] = {
                "messages_in_per_sec": _get_topic(metrics,
                    'kafka_server_brokertopicmetrics_messagesin_by_topic_total', topic),
                "bytes_in_per_sec": _get_topic(metrics,
                    'kafka_server_brokertopicmetrics_bytesin_by_topic_total', topic),
                "bytes_out_per_sec": _get_topic(metrics,
                    'kafka_server_brokertopicmetrics_bytesout_by_topic_total', topic),
                "size_bytes": int(topic_sizes.get(topic, 0)),
            }
    except Exception as exc:
        logger.warning("Topic metrics scrape failed for %s:%s: %s", host, prometheus_port, exc)
    return result
