"""Prometheus JMX Exporter scraper for Kafka broker metrics.
Pull-based intelligence — scrapes broker JMX Exporter HTTP endpoint,
extracts only the metrics we need from the response.
No RMI, no jpype, no Prometheus server — pure HTTP.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)
_SCRAPE_TIMEOUT = 30.0

# Broker metrics we extract from the full /metrics dump:
# jvm_memory_bytes_used, jvm_memory_bytes_max, process_cpu_seconds_total,
# jvm_gc_collection_seconds_sum, kafka_server_brokertopicmetrics_messagesin_total,
# kafka_server_brokertopicmetrics_bytesin_total, kafka_server_brokertopicmetrics_bytesout_total,
# kafka_server_replicamanager_underreplicatedpartitions, atminisrpartitioncount, partitioncount,
# isrshrinks_total, isrexpands_total, kafkarequesthandlerpool, requestmetrics_totaltimems

# Topic metrics we extract:
# kafka_server_brokertopicmetrics_messagesin_by_topic_total, bytesin_by_topic_total,
# bytesout_by_topic_total, kafka_log_log_size

# Per-broker persistent state for rate calculations (keyed by host:port)
_broker_state: dict[str, dict] = {}


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
    """Get metric value by name and optional label filters.
    no_labels_only=True: only match entries with no labels (broker-level aggregates)."""
    entries = metrics.get(name, [])
    if not entries:
        return 0.0
    if labels is None and no_labels_only:
        # Only return entries with no labels — broker-level aggregates
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


async def scrape_broker(host: str, port: int) -> dict[str, Any]:
    """Scrape ONE broker — only broker-level metrics, no topic data."""
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
        async with httpx.AsyncClient(timeout=_SCRAPE_TIMEOUT) as client:
            resp = await client.get(f"http://{host}:{port}/metrics")
            resp.raise_for_status()
        # Parse full dump — _get extracts only what we need, rest discarded
        metrics = _parse_prometheus_text(resp.text)

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

        # GC pause rate (ms/s)
        gc_curr = _get(metrics, 'jvm_gc_collection_seconds_sum', {'gc': 'G1 Young Generation'})
        gc_prev = _get(prev_metrics, 'jvm_gc_collection_seconds_sum',
                      {'gc': 'G1 Young Generation'}) if prev_metrics else gc_curr
        gc_ms = round(_rate(gc_curr, gc_prev, elapsed) * 1000, 1)

        # Throughput rates
        def rate_metric(name: str) -> float:
            # Use no_labels_only to get broker-level aggregate, not sum of all topic entries
            curr = _get(metrics, name, no_labels_only=True)
            prev_val = _get(prev_metrics, name, no_labels_only=True) if prev_metrics else curr
            return _rate(curr, prev_val, elapsed)

        msgs_in = rate_metric('kafka_server_brokertopicmetrics_messagesin_total')
        bytes_in = rate_metric('kafka_server_brokertopicmetrics_bytesin_total')
        bytes_out = rate_metric('kafka_server_brokertopicmetrics_bytesout_total')
        isr_shrinks = rate_metric('kafka_server_replicamanager_isrshrinks_total')
        isr_expands = rate_metric('kafka_server_replicamanager_isrexpands_total')

        # Latency — 99th percentile
        produce_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                              {'quantile': '0.999', 'request': 'Produce'})
        fetch_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                            {'quantile': '0.999', 'request': 'Fetch'})

        # Gauges
        urp = int(_get(metrics, 'kafka_server_replicamanager_underreplicatedpartitions'))
        at_min_isr = int(_get(metrics, 'kafka_server_replicamanager_atminisrpartitioncount'))
        partition_count = int(_get(metrics, 'kafka_server_replicamanager_partitioncount'))

        # Store state for next rate calculation
        _broker_state[state_key] = {'metrics': metrics, 'time': now}

        return {
            "heap_pct": heap_pct, "cpu_pct": cpu_pct, "gc_pause_ms": gc_ms,
            "messages_in_per_sec": msgs_in, "bytes_in_per_sec": bytes_in,
            "bytes_out_per_sec": bytes_out, "request_handler_idle_pct": 100.0,
            "isr_shrinks_per_sec": isr_shrinks, "isr_expands_per_sec": isr_expands,
            "produce_latency_ms": produce_latency, "fetch_latency_ms": fetch_latency,
            "under_replicated_partitions": urp, "at_min_isr_partitions": at_min_isr,
            "partition_count": partition_count,
        }
    except Exception as exc:
        logger.warning("Prometheus scrape failed for %s:%s: %s", host, port, exc)
        return defaults


async def scrape_all_brokers(brokers: list[dict], prometheus_port: int) -> dict[str, dict]:
    """Scrape all brokers in parallel — targeted metrics only."""
    tasks = []
    broker_ids = []
    for broker in brokers:
        host = broker.get("host", "")
        if not host:
            continue
        broker_ids.append(str(broker.get("broker_id", host)))
        tasks.append(scrape_broker(host, prometheus_port))

    results = {}
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    for broker_id, result in zip(broker_ids, completed):
        if isinstance(result, Exception):
            logger.warning("Broker scrape failed for %s: %s", broker_id, result)
            results[broker_id] = {}
        else:
            results[broker_id] = result
    return results


async def scrape_topic_metrics(host: str, prometheus_port: int,
                                topic_names: list[str]) -> dict[str, dict]:
    """Scrape per-topic metrics — targeted to requested topics only.
    Uses filtered URL to avoid pulling all 18k+ topic metrics."""
    result: dict[str, dict] = {}
    try:
        async with httpx.AsyncClient(timeout=_SCRAPE_TIMEOUT) as client:
            resp = await client.get(f"http://{host}:{prometheus_port}/metrics")
            resp.raise_for_status()
        metrics = _parse_prometheus_text(resp.text)

        # Build topic size map
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
                "size_bytes": topic_sizes.get(topic, 0),
            }
    except Exception as exc:
        logger.warning("Topic metrics scrape failed for %s:%s: %s", host, prometheus_port, exc)
    return result
