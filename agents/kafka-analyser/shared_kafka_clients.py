"""Shared, persistent Kafka client management -- one AdminClient per cluster,
reused across all collectors instead of each creating its own fresh connection.
Reduces connection load on both our application and the monitored brokers."""
import contextlib
import threading
import time
import logging
from kafka import KafkaAdminClient

logger = logging.getLogger(__name__)

_clients: dict[str, KafkaAdminClient] = {}
_locks: dict[str, threading.Lock] = {}
_registry_lock = threading.Lock()
_bootstrap_by_cluster: dict[str, str] = {}  # cluster_id -> bootstrap_servers, for logging at close time

# Set once at module import time, in the main process. Used to tag early
# connection events as context='startup' (mirrors the same pattern in
# kafka_process_pool.py).
_MODULE_LOAD_TIME = time.time()
_STARTUP_WINDOW_SECS = 300


def _log_connection_event(bootstrap_servers: str, event_type: str, client_type: str) -> None:
    """Best-effort audit log write for a connection create/close event. This
    function runs inside a background THREAD (via run_in_executor), not the
    main asyncio event loop, so it uses a one-off manually-managed event
    loop + a raw asyncpg connection rather than the app's normal
    SQLAlchemy async session (NOT asyncio.run() -- see the loop.close()
    comment below for why). NEVER allowed to affect the calling job: any
    failure here is
    caught and silently ignored, and the whole operation is hard-capped so
    it can't become a new source of delay in this connection-management
    code path (built following a real incident, 2026-09-24)."""
    try:
        import asyncio
        import asyncpg
        from config import settings
        if not settings.database_url:
            return
        dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
        context = "startup" if (time.time() - _MODULE_LOAD_TIME) < _STARTUP_WINDOW_SECS else "normal"

        async def _write():
            conn = await asyncpg.connect(dsn, timeout=2)
            try:
                await conn.execute(
                    "INSERT INTO kafka_connection_events "
                    "(bootstrap_servers, event_type, client_type, source, context, created_at) "
                    "VALUES ($1, $2, $3, 'shared', $4, now())",
                    bootstrap_servers, event_type, client_type, context,
                )
            finally:
                conn.terminate()

        # NOT asyncio.run() -- confirmed via live py-spy stack traces
        # (2026-09-26) that asyncio.run()'s own cleanup (Runner.close(),
        # specifically its shutdown_default_executor() wait) can hang
        # indefinitely in this exact pattern, permanently freezing the
        # calling thread on what should be a bounded, best-effort
        # logging call. A bare loop.close() does not include that wait.
        _log_loop = asyncio.new_event_loop()
        try:
            _log_loop.run_until_complete(asyncio.wait_for(_write(), timeout=3))
        finally:
            _log_loop.close()
    except Exception:
        pass


def _security_kwargs(cluster_config: dict) -> dict:
    """Build kafka-python security kwargs from a cluster config dict."""
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


def get_shared_admin_client(cluster_id: str, cluster_config: dict) -> tuple[KafkaAdminClient, threading.Lock]:
    """Return a persistent, shared KafkaAdminClient for this cluster, creating it
    on first use. Returns (client, lock) -- caller MUST acquire the lock before
    using the client, since the underlying connection is not safe for unsynchronized
    concurrent use across threads. Recreates the client automatically if it appears
    disconnected."""
    _newly_created_bootstrap = None
    with _registry_lock:
        if cluster_id not in _clients:
            security = _security_kwargs(cluster_config)
            _clients[cluster_id] = KafkaAdminClient(
                bootstrap_servers=cluster_config["bootstrap_servers"],
                request_timeout_ms=15000,
                **security,
            )
            _locks[cluster_id] = threading.Lock()
            _bootstrap_by_cluster[cluster_id] = cluster_config["bootstrap_servers"]
            logger.info("Created persistent shared AdminClient for cluster %s", cluster_id)
            _newly_created_bootstrap = cluster_config["bootstrap_servers"]
        result = (_clients[cluster_id], _locks[cluster_id])
    if _newly_created_bootstrap is not None:
        _log_connection_event(_newly_created_bootstrap, "created", "admin")
    return result


@contextlib.contextmanager
def acquire_admin_lock(cluster_id: str, lock: threading.Lock, timeout: float = 30.0):
    """Acquire a cluster's admin lock with a bounded timeout, instead of blocking
    indefinitely. Root-cause fix for a confirmed incident: asyncio.wait_for()
    cannot actually stop a thread already dispatched to run_in_executor() when its
    own timeout fires, so an orphaned thread from one job's timed-out attempt can
    keep running and holding this lock -- silently blocking a completely different
    job that shares the same cluster's AdminClient, for as long as the orphaned
    thread takes to finish. This bounds that blocking window and, on failure,
    invalidates the client so the next caller reconnects fresh rather than
    potentially queuing behind the same stuck lock again.

    Usage: replace `with admin_lock:` with
    `with acquire_admin_lock(cluster_id_or_bootstrap_servers, admin_lock):`
    """
    acquired = lock.acquire(timeout=timeout)
    if not acquired:
        invalidate_client(cluster_id)
        raise TimeoutError(
            f"Could not acquire admin lock for cluster {cluster_id} within "
            f"{timeout}s -- likely an orphaned thread from a previous timed-out "
            f"operation still holding it. Client invalidated; will reconnect "
            f"fresh on next use."
        )
    try:
        yield
    finally:
        lock.release()


def invalidate_client(cluster_id: str) -> None:
    """Force recreation of a cluster's shared client on next use -- call this if a
    client appears broken/disconnected, so the next get_shared_admin_client() call
    rebuilds it fresh rather than continuing to use a dead connection."""
    with _registry_lock:
        old = _clients.pop(cluster_id, None)
        _locks.pop(cluster_id, None)
        bootstrap_servers = _bootstrap_by_cluster.pop(cluster_id, cluster_id)
        if old:
            try:
                old.close()
            except Exception:
                pass
            logger.warning("Invalidated shared AdminClient for cluster %s -- will recreate on next use", cluster_id)
    if old:
        _log_connection_event(bootstrap_servers, "closed", "admin")


def refresh_all_shared_clients() -> dict:
    """Unconditionally refreshes every cached shared (main-process) admin
    client, on the same principle as kafka_process_pool.py's
    refresh_all_worker_clients_isolated -- proactive, not a health-test (a
    health-test approach was tried and empirically proven not to work this
    same session: describe_cluster() can succeed via a different healthy
    connection while the actual stale one is left untouched). Unlike the
    worker-process cache, this one is a single dict in the main process
    with an associated threading.Lock per cluster already used to guard
    concurrent access -- so before discarding a cluster's client, this
    tries a NON-BLOCKING lock acquisition first (lock.acquire(blocking=False)).
    If acquired, the client is genuinely idle right now and safe to
    discard; the lock is released immediately after. If NOT acquired, some
    other caller is actively using that client at this exact moment --
    skip it this cycle rather than disrupt in-flight work; it will be
    considered again on the next cycle (this function runs every 5 minutes
    via the existing kafka-breaker-recovery-check job). Returns
    {"refreshed": [...cluster_ids...], "skipped_busy": [...cluster_ids...]}
    for observability.

    Known, accepted trade-off (found during review, 2026-09-24): callers
    fetch (admin, admin_lock) via get_shared_admin_client(), THEN acquire
    admin_lock -- there's a narrow window between those two steps where
    this function could acquire the same (still-technically-free) lock
    first and discard that client. The caller would then acquire its own
    already-held reference to the old lock, use the now-closed client, and
    fail once. Its own exception handler (e.g. tools/real_kafka.py) calls
    invalidate_client immediately on that failure, not on a later retry --
    which can itself discard a fresh client a DIFFERENT caller has since
    created in the meantime, costing one extra close/create cycle beyond
    the minimum. Still self-healing (the next real attempt gets a working
    client either way), consistent with every other transient failure this
    system already tolerates and recovers from. Not fixed here (would
    require every caller to re-validate _clients.get(cluster_id) is admin
    after acquiring the lock, a broader change across every call site) --
    accepted as a rare, bounded, self-healing cost given the alternative
    (never proactively refreshing) is the actual problem being solved.

    CALLER REQUIREMENT: this function does blocking I/O (old.close(),
    plus invalidate_client's own audit-log DB write) while holding a
    per-cluster lock. It must be called via run_in_executor from a
    background thread, never directly on the asyncio event loop. This
    module's functions were confirmed to warn/fail when called directly on
    an already-running loop (asyncio.run() cannot be nested): a manual
    diagnostic script (2026-09-24, testing describe_cluster() against
    cluster 4's shared client) produced
    "RuntimeWarning: coroutine 'wait_for' was never awaited" and
    "RuntimeWarning: coroutine '_log_connection_event.<locals>._write' was
    never awaited" when get_shared_admin_client() was called directly
    inside an existing asyncio.run() block, rather than via a background
    thread."""
    results: dict = {"refreshed": [], "skipped_busy": []}
    with _registry_lock:
        cluster_ids = list(_clients.keys())
    for cluster_id in cluster_ids:
        with _registry_lock:
            lock = _locks.get(cluster_id)
        if lock is None:
            continue
        acquired = lock.acquire(blocking=False)
        if not acquired:
            results["skipped_busy"].append(cluster_id)
            continue
        try:
            invalidate_client(cluster_id)
            results["refreshed"].append(cluster_id)
        finally:
            lock.release()
    return results
