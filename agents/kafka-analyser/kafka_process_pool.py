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
