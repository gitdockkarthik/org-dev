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
                        if (meta.offset if hasattr(meta, 'offset') else meta) > 0
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
