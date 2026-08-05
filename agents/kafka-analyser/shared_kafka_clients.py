"""Shared, persistent Kafka client management -- one AdminClient per cluster,
reused across all collectors instead of each creating its own fresh connection.
Reduces connection load on both our application and the monitored brokers."""
import contextlib
import threading
import logging
from kafka import KafkaAdminClient

logger = logging.getLogger(__name__)

_clients: dict[str, KafkaAdminClient] = {}
_locks: dict[str, threading.Lock] = {}
_registry_lock = threading.Lock()


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
    with _registry_lock:
        if cluster_id not in _clients:
            security = _security_kwargs(cluster_config)
            _clients[cluster_id] = KafkaAdminClient(
                bootstrap_servers=cluster_config["bootstrap_servers"],
                request_timeout_ms=15000,
                **security,
            )
            _locks[cluster_id] = threading.Lock()
            logger.info("Created persistent shared AdminClient for cluster %s", cluster_id)
        return _clients[cluster_id], _locks[cluster_id]


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
        if old:
            try:
                old.close()
            except Exception:
                pass
            logger.warning("Invalidated shared AdminClient for cluster %s -- will recreate on next use", cluster_id)
