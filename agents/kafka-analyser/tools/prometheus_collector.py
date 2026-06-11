"""Prometheus JMX Exporter scraper for Kafka broker metrics.
Scrapes the JMX Prometheus Java agent HTTP endpoint directly on each broker.
No jpype, no RMI, no Prometheus server needed — pure HTTP.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)
_SCRAPE_TIMEOUT = 5.0

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


def _get(metrics: dict, name: str, labels: dict | None = None) -> float:
    """Get metric value by name and optional label filters. Returns 0.0 if not found."""
    entries = metrics.get(name, [])
    if not entries:
        return 0.0
    if not labels:
        # Sum all entries if no label filter (e.g. broker-level totals)
        return sum(e['value'] for e in entries)
    for entry in entries:
        if all(entry['labels'].get(k) == v for k, v in labels.items()):
            return entry['value']
    return 0.0


def _get_topic(metrics: dict, name: str, topic: str) -> float:
    """Get per-topic metric value."""
    return _get(metrics, name, {'topic': topic})


def _rate(curr: float, prev: float, elapsed: float) -> float:
    """Compute per-second rate from two counter values."""
    if elapsed <= 0 or curr < prev:
        return 0.0
    return round((curr - prev) / elapsed, 2)


async def scrape_broker(host: str, port: int) -> dict[str, Any]:
    """Scrape one broker's Prometheus endpoint and return broker metrics dict."""
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
        metrics = _parse_prometheus_text(resp.text)

        prev = _broker_state.get(state_key, {})
        prev_metrics = prev.get('metrics', {})
        prev_time = prev.get('time', now)
        elapsed = now - prev_time if prev_time != now else 60.0

        # Heap %
        heap_used = _get(metrics, 'jvm_memory_bytes_used', {'area': 'heap'})
        heap_max = _get(metrics, 'jvm_memory_bytes_max', {'area': 'heap'})
        heap_pct = round((heap_used / heap_max) * 100, 1) if heap_max > 0 else 0.0

        # CPU % — rate of process_cpu_seconds_total
        cpu_curr = _get(metrics, 'process_cpu_seconds_total')
        cpu_prev = _get(prev_metrics, 'process_cpu_seconds_total') if prev_metrics else cpu_curr
        cpu_pct = min(100.0, max(0.0, round(_rate(cpu_curr, cpu_prev, elapsed) * 100, 1)))

        # GC pause rate (ms/s)
        gc_curr = _get(metrics, 'jvm_gc_collection_seconds_sum', {'gc': 'G1 Young Generation'})
        gc_prev = _get(prev_metrics, 'jvm_gc_collection_seconds_sum',
                      {'gc': 'G1 Young Generation'}) if prev_metrics else gc_curr
        gc_ms = round(_rate(gc_curr, gc_prev, elapsed) * 1000, 1)

        # Throughput rates
        msgs_curr = _get(metrics, 'kafka_server_brokertopicmetrics_messagesin_total')
        msgs_prev = _get(prev_metrics, 'kafka_server_brokertopicmetrics_messagesin_total') if prev_metrics else msgs_curr
        msgs_in = _rate(msgs_curr, msgs_prev, elapsed)

        bin_curr = _get(metrics, 'kafka_server_brokertopicmetrics_bytesin_total')
        bin_prev = _get(prev_metrics, 'kafka_server_brokertopicmetrics_bytesin_total') if prev_metrics else bin_curr
        bytes_in = _rate(bin_curr, bin_prev, elapsed)

        bout_curr = _get(metrics, 'kafka_server_brokertopicmetrics_bytesout_total')
        bout_prev = _get(prev_metrics, 'kafka_server_brokertopicmetrics_bytesout_total') if prev_metrics else bout_curr
        bytes_out = _rate(bout_curr, bout_prev, elapsed)

        # ISR rates
        isrs_curr = _get(metrics, 'kafka_server_replicamanager_isrshrinks_total')
        isrs_prev = _get(prev_metrics, 'kafka_server_replicamanager_isrshrinks_total') if prev_metrics else isrs_curr
        isr_shrinks = _rate(isrs_curr, isrs_prev, elapsed)

        isre_curr = _get(metrics, 'kafka_server_replicamanager_isrexpands_total')
        isre_prev = _get(prev_metrics, 'kafka_server_replicamanager_isrexpands_total') if prev_metrics else isre_curr
        isr_expands = _rate(isre_curr, isre_prev, elapsed)

        # Latency — 99th percentile
        produce_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                              {'quantile': '0.999', 'request': 'Produce'})
        fetch_latency = _get(metrics, 'kafka_network_requestmetrics_totaltimems',
                            {'quantile': '0.999', 'request': 'Fetch'})

        # Gauges — direct values
        urp = int(_get(metrics, 'kafka_server_replicamanager_underreplicatedpartitions'))
        at_min_isr = int(_get(metrics, 'kafka_server_replicamanager_atminisrpartitioncount'))
        partition_count = int(_get(metrics, 'kafka_server_replicamanager_partitioncount'))

        # Store state for next rate calculation
        _broker_state[state_key] = {'metrics': metrics, 'time': now}

        return {
            "heap_pct": heap_pct,
            "cpu_pct": cpu_pct,
            "gc_pause_ms": gc_ms,
            "messages_in_per_sec": msgs_in,
            "bytes_in_per_sec": bytes_in,
            "bytes_out_per_sec": bytes_out,
            "request_handler_idle_pct": 100.0,
            "isr_shrinks_per_sec": isr_shrinks,
            "isr_expands_per_sec": isr_expands,
            "produce_latency_ms": produce_latency,
            "fetch_latency_ms": fetch_latency,
            "under_replicated_partitions": urp,
            "at_min_isr_partitions": at_min_isr,
            "partition_count": partition_count,
        }
    except Exception as exc:
        logger.warning("Prometheus scrape failed for %s:%s: %s", host, port, exc)
        return defaults


async def scrape_all_brokers(brokers: list[dict], prometheus_port: int) -> dict[str, dict]:
    """Scrape all brokers in parallel. Returns {broker_id: metrics_dict}."""
    tasks = []
    broker_ids = []
    for broker in brokers:
        host = broker.get("host", "")
        if not host:
            continue
        broker_id = str(broker.get("broker_id", host))
        broker_ids.append(broker_id)
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
    """Scrape per-topic metrics for specified topics from one broker."""
    result: dict[str, dict] = {}
    try:
        async with httpx.AsyncClient(timeout=_SCRAPE_TIMEOUT) as client:
            resp = await client.get(f"http://{host}:{prometheus_port}/metrics")
            resp.raise_for_status()
        metrics = _parse_prometheus_text(resp.text)

        # Build topic size map from log size metric
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
