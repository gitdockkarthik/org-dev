"""Process-based isolation for Kafka calls that have caused real, confirmed
reliability incidents when run as threads. Threads dispatched to
run_in_executor() cannot be forcibly stopped once their own timeout fires (a
genuine Python limitation) -- an orphaned thread can keep running indefinitely,
holding shared locks and blocking unrelated jobs. Processes CAN be forcibly
killed on timeout regardless of what they're internally stuck on. This is a
small, dedicated pool for specific CRITICAL operations only -- NOT a wholesale
replacement of the main thread pool used by everything else, to protect the
memory headroom gained from the recent infrastructure upgrade."""
import logging
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeoutError

logger = logging.getLogger(__name__)

# Small, dedicated pool for CRITICAL, lock-blocking-prone operations only.
# Reverted to 4 (2026-08-08): initially increased to 6 based on apparent pool
# contention, but further investigation found the real root cause of the same
# symptom (consumer-lag-8 intermittent timeouts) was host-wide CPU starvation
# from an unrelated ClickHouse internal-log accumulation issue (see
# SESSION_RECONCILIATION.md, 2026-08-08), not this pool's size. Reverted per the
# principle of minimal, evidence-justified changes -- job-schedule staggering
# (kafka_job_schedules) plus the ClickHouse fix fully resolved the observed
# failures; this pool size increase was not actually needed.
_process_pool = ProcessPoolExecutor(max_workers=4)

# Per-worker-process persistent connections. Each worker process has its own
# separate memory space, so this dict is naturally process-local -- no locking
# needed within a single worker (each worker handles one task at a time).
_worker_clients: dict[str, "KafkaAdminClient"] = {}


def _build_security_kwargs(cluster_config: dict) -> dict:
    """Build kafka-python security kwargs -- including the ssl.SSLContext object
    -- ENTIRELY INSIDE the worker process. SSLContext objects cannot be pickled,
    so cluster_config must only ever contain plain, picklable fields (strings/
    booleans) when crossing the process boundary; this function builds the real,
    non-picklable security dict locally, where it's only ever used."""
    security = {}
    if cluster_config.get("auth_type") not in (None, "none"):
        import ssl
        tls = cluster_config.get("tls_enabled", False)
        security = {
            "security_protocol": "SASL_SSL" if tls else "SASL_PLAINTEXT",
            "sasl_mechanism": cluster_config.get("sasl_mechanism", "PLAIN"),
            "sasl_plain_username": cluster_config.get("sasl_username"),
            "sasl_plain_password": cluster_config.get("sasl_password"),
        }
        if tls:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            security["ssl_context"] = ssl_ctx
    return security


def _get_worker_client(bootstrap_servers: str, cluster_config: dict):
    """Get or create this WORKER PROCESS's own persistent AdminClient for a
    cluster. Runs inside the worker process, not the main process. Builds
    security kwargs (including any ssl_context) locally -- never pickled."""
    from kafka import KafkaAdminClient
    if bootstrap_servers not in _worker_clients:
        security = _build_security_kwargs(cluster_config)
        _worker_clients[bootstrap_servers] = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=15000,
            **security,
        )
    return _worker_clients[bootstrap_servers]


def _describe_log_dirs_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS (not thread) -- can be forcibly killed on
    timeout by the main process, unlike a thread. cluster_config contains only
    plain, picklable fields (auth_type, tls_enabled, sasl_username,
    sasl_password, sasl_mechanism); the real security kwargs (including
    ssl_context) are built locally inside this worker. Returns a plain dict
    (picklable; kafka-python's own response objects are not)."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        result = admin.describe_log_dirs()
        sizes: dict[str, int] = {}
        for log_dir in result.log_dirs:
            if log_dir[0] != 0:
                continue
            for topic_entry in log_dir[2]:
                topic = topic_entry[0]
                if topic.startswith('_'):
                    continue
                for partition in topic_entry[1]:
                    key = f"{topic}:{partition[0]}"
                    sizes[key] = partition[1]
        return {"ok": True, "sizes": sizes}
    except Exception as exc:
        # This worker's connection may be in a bad state -- drop it so the next
        # task on this worker builds a fresh one, rather than reusing a possibly
        # broken connection.
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_log_dirs_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 30.0) -> dict:
    """Main-process-side entry point. cluster_config must contain only plain,
    picklable fields (auth_type, tls_enabled, sasl_username, sasl_password,
    sasl_mechanism) -- NOT a pre-built security dict, since that could contain
    an unpicklable ssl.SSLContext object. Dispatches describe_log_dirs to the
    dedicated process pool with a hard timeout -- if the worker process doesn't
    respond in time, it is forcibly killed (not just abandoned like a thread
    would be), and the pool automatically spins up a replacement worker for
    future calls. Returns {"ok": True, "sizes": {...}} on success, or
    {"ok": False, "error": "..."} on failure/timeout -- callers should NOT raise
    on a False result, just treat it as "no data this cycle", matching the
    existing baseline-storage pattern in collect_msg_rate."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_log_dirs_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_log_dirs_isolated: worker process did not respond within "
            "%ss for %s -- killing and replacing it (this is the intended, safe "
            "behavior; a thread-based call could not have been stopped this way)",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_log_dirs_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _get_worker_consumer_client(bootstrap_servers: str, cluster_config: dict):
    """Get or create this WORKER PROCESS's own persistent KafkaConsumer for
    fetching end offsets. Separate from the AdminClient used for
    list_consumer_groups/list_consumer_group_offsets."""
    from kafka import KafkaConsumer
    key = f"consumer:{bootstrap_servers}"
    if key not in _worker_clients:
        security = _build_security_kwargs(cluster_config)
        _worker_clients[key] = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000,
            **security,
        )
    return _worker_clients[key]


def _fetch_consumer_lag_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS -- the full consumer-lag workflow (list
    groups, fetch committed offsets, fetch end offsets via a KafkaConsumer
    session, compute lag), moved here as a single unit from collectors.py's
    _fetch_all_lags() to avoid IPC overhead from splitting a workflow whose
    intermediate data can be large. No admin_lock needed -- this worker process
    has exclusive use of its own connection."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        all_groups = admin.list_consumer_groups()
        consumer_gids = [g[0] for g in all_groups if g[1] == "consumer"]
        connect_gids = [g[0] for g in all_groups if g[1] == "connect"]
        sr_gids = [g[0] for g in all_groups if g[1] == "sr"]
        empty_gids = [g[0] for g in all_groups if g[1] == ""]
        lag_target_gids = consumer_gids + connect_gids
        enriched = []
        total_lag = 0
        BATCH = 100
        group_committed = {}
        end_offsets = {}
        for batch_start in range(0, len(lag_target_gids), BATCH):
            batch_gids = lag_target_gids[batch_start:batch_start + BATCH]
            for gid in batch_gids:
                try:
                    offsets = admin.list_consumer_group_offsets(gid)
                    group_committed[gid] = {
                        tp: (meta.offset if hasattr(meta, 'offset') else meta)
                        for tp, meta in offsets.items()
                        if (meta.offset if hasattr(meta, 'offset') else meta) >= 0
                    }
                except Exception:
                    group_committed[gid] = {}
        all_tps = list(set(tp for committed in group_committed.values() for tp in committed.keys()))
        if all_tps:
            _SEEK_MAX_ATTEMPTS = 3
            _seek_last_exc = None
            for _attempt in range(1, _SEEK_MAX_ATTEMPTS + 1):
                try:
                    _consumer = _get_worker_consumer_client(bootstrap_servers, cluster_config)
                    SEEK_BATCH = 500
                    for i in range(0, len(all_tps), SEEK_BATCH):
                        batch_tps = all_tps[i:i + SEEK_BATCH]
                        _consumer.assign(batch_tps)
                        _consumer.seek_to_end(*batch_tps)
                        end_offsets.update({tp: _consumer.position(tp) for tp in batch_tps})
                    _seek_last_exc = None
                    break
                except Exception as e:
                    _seek_last_exc = e
                    _worker_clients.pop(f"consumer:{bootstrap_servers}", None)
                    end_offsets.clear()
                    if _attempt < _SEEK_MAX_ATTEMPTS:
                        import time as _time
                        _time.sleep(2)
            if _seek_last_exc is not None:
                return {"ok": False, "error": f"end-offset fetch failed after {_SEEK_MAX_ATTEMPTS} attempts: {_seek_last_exc}"}
        group_topic_lag: dict[str, dict[str, dict]] = {}
        group_partition_lag: dict[str, list[dict]] = {}
        for gid in lag_target_gids:
            committed = group_committed.get(gid, {})
            group_lag = sum(
                max(0, end_offsets.get(tp, committed_off) - committed_off)
                for tp, committed_off in committed.items()
            )
            enriched.append({
                "group_id": gid,
                "state": "connect" if gid in connect_gids else "consumer",
                "topic_count": len(set(tp.topic for tp in committed.keys())),
                "total_lag": group_lag,
                "committed_offsets": len(committed),
            })
            total_lag += group_lag
            topic_agg: dict[str, dict] = {}
            for tp, committed_off in committed.items():
                partition_lag = max(0, end_offsets.get(tp, committed_off) - committed_off)
                entry = topic_agg.setdefault(tp.topic, {"lag": 0, "partitions": 0})
                entry["lag"] += partition_lag
                entry["partitions"] += 1
                end_off = end_offsets.get(tp, committed_off)
                group_partition_lag.setdefault(gid, []).append({
                    "topic": tp.topic, "partition": tp.partition, "lag": partition_lag,
                    "end_offset": end_off, "committed_offset": committed_off,
                })
            group_topic_lag[gid] = topic_agg
        return {
            "ok": True,
            "groups": enriched,
            "group_states": {
                "consumer": len(consumer_gids),
                "connect": len(connect_gids),
                "schema_registry": len(sr_gids),
                "empty": len(empty_gids),
                "total": len(all_groups),
            },
            "total_lag": total_lag,
            "group_topic_lag": group_topic_lag,
            "group_partition_lag": group_partition_lag,
        }
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def fetch_consumer_lag_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 60.0) -> dict:
    """Main-process-side entry point for the full consumer-lag workflow.
    cluster_config must contain only plain, picklable fields, matching
    describe_log_dirs_isolated(). Returns {"ok": True, "groups": [...],
    "group_states": {...}, "total_lag": N, "group_topic_lag": {...},
    "group_partition_lag": {...}} on success, or {"ok": False, "error": "..."}
    on failure/timeout."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_fetch_consumer_lag_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "fetch_consumer_lag_isolated: worker process did not respond within "
            "%ss for %s -- killing and replacing it",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("fetch_consumer_lag_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _describe_cluster_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS. Returns the plain dict from kafka-python's
    own describe_cluster() (already picklable via its internal .to_object())."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        cluster_info = admin.describe_cluster()
        return {"ok": True, "cluster_info": cluster_info}
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_cluster_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 30.0) -> dict:
    """Main-process-side entry point for a simple describe_cluster() call.
    Returns {"ok": True, "cluster_info": {...}} on success, or
    {"ok": False, "error": "..."} on failure/timeout. Caller is responsible for
    any further transformation (e.g. RealKafkaCollector._build_brokers(), which
    also does Prometheus/JMX scraping -- kept in the main process since it's not
    part of the risky, lock-holding Kafka call)."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_cluster_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_cluster_isolated: worker process did not respond within "
            "%ss for %s -- killing and replacing it",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_cluster_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _describe_broker_log_dirs_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS -- describe_cluster() for node_ids, then a
    per-node DescribeLogDirsRequest loop, moved here as a single unit since
    the node loop depends on node_ids from the initial describe_cluster() call."""
    from kafka.protocol.admin import DescribeLogDirsRequest
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        cluster_info = admin.describe_cluster()
        node_ids = [node.get("node_id") for node in cluster_info.get("brokers", []) or []]
        broker_sizes: dict[int, int | None] = {}
        version = admin._matching_api_version(DescribeLogDirsRequest)
        for node_id in node_ids:
            try:
                request = DescribeLogDirsRequest[version]()
                future = admin._send_request_to_node(node_id, request)
                admin._wait_for_futures([future])
                result = future.value
                total = 0
                for log_dir in result.log_dirs:
                    if log_dir[0] != 0:
                        continue
                    for topic_entry in log_dir[2]:
                        for partition in topic_entry[1]:
                            total += partition[1]
                broker_sizes[node_id] = total
            except Exception as exc:
                broker_sizes[node_id] = None
        return {
            "ok": True,
            "broker_sizes": broker_sizes,
            "broker_sizes_gb": {
                k: (round(v / 1024**3, 2) if v is not None else None)
                for k, v in broker_sizes.items()
            },
        }
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_broker_log_dirs_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 30.0) -> dict:
    """Main-process-side entry point for the full broker-log-dir-sizes workflow
    (describe_cluster + per-node DescribeLogDirsRequest loop). Returns
    {"ok": True, "broker_sizes": {...}, "broker_sizes_gb": {...}} on success, or
    {"ok": False, "error": "..."} on failure/timeout."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_broker_log_dirs_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_broker_log_dirs_isolated: worker process did not respond "
            "within %ss for %s -- killing and replacing it",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_broker_log_dirs_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _describe_topics_chunk_worker(bootstrap_servers: str, cluster_config: dict, topic_names: list[str]) -> dict:
    """Runs inside a worker PROCESS -- describes one chunk of topics (internally
    batched at 500 per describe_topics() call, matching the existing behavior).
    No admin_lock needed -- this worker process has exclusive use of its own
    connection, unlike the original thread-based version where all chunks shared
    one lock and were effectively serialized through it anyway."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        described = []
        _BATCH = 500
        for i in range(0, len(topic_names), _BATCH):
            described.extend(admin.describe_topics(topic_names[i:i + _BATCH]))
        topics = []
        for meta in described:
            name = meta.get("topic", "")
            if not name:
                continue
            partitions = meta.get("partitions", []) or []
            partition_count = len(partitions)
            replication_factor = len(partitions[0].get("replicas", [])) if partitions else 0
            urp = 0
            for part in partitions:
                replicas = part.get("replicas", []) or []
                isr = part.get("isr", []) or []
                if len(isr) < len(replicas):
                    urp += 1
            topics.append({
                "name": name,
                "partition_count": partition_count,
                "replication_factor": replication_factor,
                "messages_in_per_sec": 0.0,
                "bytes_in_per_sec": 0.0,
                "bytes_out_per_sec": 0.0,
                "total_messages": 0,
                "size_bytes": 0,
                "retention_bytes": -1,
                "retention_pct": 0.0,
                "under_replicated": urp,
                "status": "degraded" if urp else "healthy",
            })
        return {"ok": True, "topics": topics}
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_topics_chunk_isolated(bootstrap_servers: str, cluster_config: dict, topic_names: list[str], timeout: float = 30.0) -> dict:
    """Main-process-side entry point for describing one chunk of topics.
    Returns {"ok": True, "topics": [...]} on success, or
    {"ok": False, "error": "..."} on failure/timeout. Caller dispatches multiple
    chunks as separate calls -- the shared 4-worker process pool queues them
    naturally, each with its own independent connection (no lock contention
    between chunks, unlike the original thread-based design)."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_topics_chunk_worker, bootstrap_servers, cluster_config, topic_names)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_topics_chunk_isolated: worker process did not respond "
            "within %ss for %s (%d topics) -- killing and replacing it",
            timeout, bootstrap_servers, len(topic_names),
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_topics_chunk_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _fetch_group_lags_worker(bootstrap_servers: str, cluster_config: dict, group_ids: list[str]) -> dict:
    """Runs inside a worker PROCESS -- fetches lag for a specific set of
    consumer/connect group_ids (on-demand lookups, startup warm-up, governance
    audit -- NOT the scheduled consumer-lag sweep, which has its own isolated
    function). No admin_lock needed -- this worker process has exclusive use of
    its own connection."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    consumer = None
    try:
        groups = []
        for gid in group_ids:
            try:
                offsets = admin.list_consumer_group_offsets(gid)
                if not offsets:
                    groups.append({
                        "group_id": gid,
                        "total_lag": 0,
                        "topic_count": 0,
                        "partitions": [],
                    })
                    continue
                if consumer is None:
                    consumer = _get_worker_consumer_client(bootstrap_servers, cluster_config)
                tps = list(offsets.keys())
                # Proven pattern (matches the working scheduled collector) --
                # bulk end_offsets() is unreliable on this broker version and
                # can hang/timeout even for a small partition count.
                end_offsets = {}
                SEEK_BATCH = 500
                for i in range(0, len(tps), SEEK_BATCH):
                    batch_tps = tps[i:i + SEEK_BATCH]
                    consumer.assign(batch_tps)
                    consumer.seek_to_end(*batch_tps)
                    end_offsets.update({tp: consumer.position(tp) for tp in batch_tps})
                partitions = []
                total_lag = 0
                topics = set()
                for tp in tps:
                    consumer_offset = offsets[tp].offset
                    log_end_offset = end_offsets.get(tp, consumer_offset)
                    lag = max(0, log_end_offset - consumer_offset) if consumer_offset >= 0 else 0
                    total_lag += lag
                    topics.add(tp.topic)
                    partitions.append({
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "lag": lag,
                        "log_end_offset": log_end_offset,
                        "consumer_offset": consumer_offset,
                    })
                groups.append({
                    "group_id": gid,
                    "total_lag": total_lag,
                    "topic_count": len(topics),
                    "partitions": partitions,
                })
            except Exception as exc:
                groups.append({
                    "group_id": gid,
                    "total_lag": -1,
                    "topic_count": 0,
                    "partitions": [],
                    "error": str(exc),
                })
        return {"ok": True, "groups": groups}
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        _worker_clients.pop(f"consumer:{bootstrap_servers}", None)
        return {"ok": False, "error": str(exc)}


async def fetch_group_lags_isolated(bootstrap_servers: str, cluster_config: dict, group_ids: list[str], timeout: float = 30.0) -> dict:
    """Main-process-side entry point for fetching lag on a specific set of
    group_ids. Returns {"ok": True, "groups": [...]} on success, or
    {"ok": False, "error": "..."} on failure/timeout. Caller can dispatch
    multiple chunks of group_ids as separate calls, letting the pool queue them
    naturally, matching the pattern already used for describe_topics_chunk_isolated."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_fetch_group_lags_worker, bootstrap_servers, cluster_config, group_ids)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "fetch_group_lags_isolated: worker process did not respond within "
            "%ss for %s (%d groups) -- killing and replacing it",
            timeout, bootstrap_servers, len(group_ids),
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("fetch_group_lags_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _describe_group_worker(bootstrap_servers: str, cluster_config: dict, group_id: str) -> dict:
    """Runs inside a worker PROCESS -- describes a single consumer group's live
    Kafka state and real topic subscriptions (from active member metadata),
    for the on-demand lookup only. Not used by any scheduled job. No
    admin_lock needed -- this worker process has exclusive use of its own
    connection."""
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        groups = admin.describe_consumer_groups([group_id])
        if not groups:
            return {"ok": False, "error": "group not found"}
        g = groups[0]
        topics = set()
        for m in g.members:
            try:
                topics.update(m.member_metadata.subscription)
            except Exception:
                continue
        return {"ok": True, "state": g.state, "member_count": len(g.members), "subscribed_topics": sorted(topics)}
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_group_isolated(bootstrap_servers: str, cluster_config: dict, group_id: str, timeout: float = 15.0) -> dict:
    """Main-process-side entry point for describing a single consumer group's
    live state. Returns {"ok": True, "state": ..., "subscribed_topics": [...]}
    on success, or {"ok": False, "error": "..."} on failure/timeout."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_group_worker, bootstrap_servers, cluster_config, group_id)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_group_isolated: worker process did not respond within %ss for %s / %s -- killing and replacing it",
            timeout, bootstrap_servers, group_id,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_group_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _seek_to_end_worker(bootstrap_servers: str, cluster_config: dict, partitions: list[tuple[str, int]]) -> dict:
    """Runs inside a worker PROCESS -- seeks to end offset for the given
    topic-partitions, batched to avoid oversized single assign() calls. Used by
    collect_topic_message_inflow. No admin_lock needed -- this worker process
    has exclusive use of its own connection."""
    from kafka import TopicPartition
    consumer = _get_worker_consumer_client(bootstrap_servers, cluster_config)
    try:
        tps = [TopicPartition(t, p) for t, p in partitions]
        end_offsets: dict = {}
        SEEK_BATCH = 500
        for i in range(0, len(tps), SEEK_BATCH):
            batch_tps = tps[i:i + SEEK_BATCH]
            consumer.assign(batch_tps)
            consumer.seek_to_end(*batch_tps)
            for tp in batch_tps:
                end_offsets[(tp.topic, tp.partition)] = consumer.position(tp)
        return {"ok": True, "end_offsets": end_offsets}
    except Exception as exc:
        _worker_clients.pop(f"consumer:{bootstrap_servers}", None)
        return {"ok": False, "error": str(exc)}


async def seek_to_end_isolated(bootstrap_servers: str, cluster_config: dict, partitions: list[tuple[str, int]], timeout: float = 120.0) -> dict:
    """Main-process-side entry point for seeking to end offset on a full
    partition list. Returns {"ok": True, "end_offsets": {(topic, partition): offset}}
    on success, or {"ok": False, "error": "..."} on failure/timeout. A timeout here
    kills and replaces the stuck worker PROCESS -- unlike the previous
    run_in_executor(None, ...) pattern, where a stuck thread could not be
    cancelled and kept running well past its own timeout, consuming memory/CPU
    indefinitely (confirmed live incident, 2026-08-08)."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_seek_to_end_worker, bootstrap_servers, cluster_config, partitions)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "seek_to_end_isolated: worker process did not respond within "
            "%ss for %s (%d partitions) -- killing and replacing it",
            timeout, bootstrap_servers, len(partitions),
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("seek_to_end_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _list_topics_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS -- lists all non-internal topic names."""
    from tools.real_kafka import _is_internal_topic
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        names = [t for t in admin.list_topics() if not _is_internal_topic(t)]
        return {"ok": True, "topics": names}
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def list_topics_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 30.0) -> dict:
    """Main-process-side entry point for listing all non-internal topics.
    Returns {"ok": True, "topics": [...]} on success, or
    {"ok": False, "error": "..."} on failure/timeout."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_list_topics_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "list_topics_isolated: worker process did not respond within %ss for %s "
            "-- killing and replacing it",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("list_topics_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}


def _describe_broker_distribution_worker(bootstrap_servers: str, cluster_config: dict) -> dict:
    """Runs inside a worker PROCESS -- lists non-internal topics, then batched
    describe_topics() for leader/replica info, aggregating leader_counts,
    replica_counts, and partition_leaders entirely within the worker to
    minimize IPC (only the aggregated result crosses the process boundary, not
    raw per-topic metadata for potentially tens of thousands of partitions)."""
    from tools.real_kafka import _is_internal_topic
    import collections
    admin = _get_worker_client(bootstrap_servers, cluster_config)
    try:
        all_topics = [t for t in admin.list_topics() if not _is_internal_topic(t)]
        leader_counts: dict = collections.defaultdict(int)
        replica_counts: dict = collections.defaultdict(int)
        partition_leaders = []
        BATCH = 500
        for i in range(0, len(all_topics), BATCH):
            meta = admin.describe_topics(all_topics[i:i + BATCH])
            for tm in meta:
                topic_name = tm.get("topic", "")
                for p in tm.get("partitions", []):
                    leader_id = str(p["leader"])
                    if leader_id != "-1":
                        leader_counts[leader_id] += 1
                    partition_leaders.append({
                        "topic": topic_name,
                        "partition": p["partition"],
                        "leader": leader_id,
                    })
                    for r in p.get("replicas", []):
                        replica_counts[str(r)] += 1
        return {
            "ok": True,
            "leader_counts": dict(leader_counts),
            "replica_counts": dict(replica_counts),
            "partition_leaders": partition_leaders,
        }
    except Exception as exc:
        _worker_clients.pop(bootstrap_servers, None)
        return {"ok": False, "error": str(exc)}


async def describe_broker_distribution_isolated(bootstrap_servers: str, cluster_config: dict, timeout: float = 90.0) -> dict:
    """Main-process-side entry point for the broker leader/replica distribution
    sweep. Returns {"ok": True, "leader_counts": {...}, "replica_counts": {...},
    "partition_leaders": [...]} on success, or {"ok": False, "error": "..."} on
    failure/timeout. A timeout here kills and replaces the stuck worker
    PROCESS -- replacing the previous shared-admin-client/admin_lock pattern,
    which a prior confirmed incident showed can let one job's stuck thread
    silently block an unrelated job sharing the same lock."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        future = _process_pool.submit(_describe_broker_distribution_worker, bootstrap_servers, cluster_config)
        result = await loop.run_in_executor(None, future.result, timeout)
        return result
    except FutureTimeoutError:
        logger.warning(
            "describe_broker_distribution_isolated: worker process did not respond "
            "within %ss for %s -- killing and replacing it",
            timeout, bootstrap_servers,
        )
        future.cancel()
        return {"ok": False, "error": f"Timed out after {timeout}s (worker process killed)"}
    except Exception as exc:
        logger.warning("describe_broker_distribution_isolated: unexpected error: %s", exc)
        return {"ok": False, "error": str(exc)}
