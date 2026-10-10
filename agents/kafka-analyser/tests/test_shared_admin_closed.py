"""A shared admin client closed by another thread must make its user fail
once, not spin. Before the fix, kafka-python-ng's _send_request_to_node loops
forever on a closed client: poll() returns at once and ready() calls wakeup()
on the closed socketpair, logging "Unable to send to wakeup socket!" ~20k
times a second at 100% CPU (reproduced 2026-10-10).

Uses a local fake broker (_fake_kafka_broker.py) on 127.0.0.1; no real
cluster, no database. Plain asserts, no pytest needed. The first checks pin
the library internals the guard relies on and fail loudly if they change. Run inside the agent
image with the agent dir on PYTHONPATH, e.g.:
  docker run --rm --network none -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_shared_admin_closed.py
Exits 0 when every check passes, 1 otherwise (os._exit, so a spinning thread
from a failing run cannot keep the process alive)."""
import inspect
import logging
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _fake_kafka_broker import FakeBroker, blackhole_port  # noqa: E402
import kafka  # noqa: E402
from kafka import KafkaAdminClient  # noqa: E402
from kafka.errors import KafkaConnectionError  # noqa: E402

import shared_kafka_clients as skc  # noqa: E402

DEADLINE_SECS = 5
WAKEUPS = []
RESULTS = []


class _Wakeups(logging.Handler):
    def emit(self, record):
        if "wakeup socket" in record.getMessage():
            WAKEUPS.append(record.threadName)


logging.basicConfig(level=logging.WARNING, stream=open(os.devnull, "w"))
logging.getLogger().addHandler(_Wakeups())


def check(name, cond, detail=""):
    RESULTS.append(bool(cond))
    print(f"{'PASS' if cond else 'FAIL'} {name}{': ' + detail if detail else ''}", flush=True)


def cfg(port):
    return {"bootstrap_servers": f"127.0.0.1:{port}", "auth_type": "none"}


def add_unreachable_node(broker, admin):
    """List a node 1 whose TCP connect never completes, and refresh the
    client's metadata so it knows it. Added after the client is built: with
    the node in the bootstrap metadata the stock KafkaAdminClient constructor
    itself fails (NodeNotReadyError) in about half the runs."""
    broker.extra_brokers.append((1, blackhole_port()))
    future = admin._client.cluster.request_update()
    deadline = time.time() + 5
    while not future.is_done and time.time() < deadline:
        admin._client.poll(timeout_ms=100)
    nodes = sorted(b.nodeId for b in admin._client.cluster.brokers())
    assert nodes == [0, 1], f"metadata refresh did not pick up node 1: {nodes}"


def use_in_thread(admin, lock, cluster_id, name):
    """Run admin.list_consumer_groups() under acquire_admin_lock in a thread,
    as tools/real_kafka.py does; returns (thread, outcome dict)."""
    outcome = {}

    def run():
        try:
            with skc.acquire_admin_lock(cluster_id, lock):
                outcome["value"] = admin.list_consumer_groups()
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    t = threading.Thread(target=run, name=name, daemon=True)
    t.start()
    return t, outcome


def finished_with_connection_error(name, t, outcome, started):
    t.join(DEADLINE_SECS)
    took = time.time() - started
    spins = WAKEUPS.count(t.name)
    check(f"{name}: caller returns within {DEADLINE_SECS} s", not t.is_alive(),
          f"alive={t.is_alive()} after {took:.1f}s, {spins} wakeup warnings")
    check(f"{name}: caller gets KafkaConnectionError", isinstance(outcome.get("error"), KafkaConnectionError),
          repr(outcome))
    check(f"{name}: at most one wakeup warning", spins <= 1, f"{spins}")


def test_library_contract():
    """The guard depends on kafka-python-ng internals. If a library change
    removes or bypasses them, the busy loop would come back silently (the
    guard's getattr falls back to "not closed"), so fail loudly here."""
    def contract(name, cond, info):
        check(f"contract: {name}", cond, info if cond else
              f"{info} -- LIBRARY CONTRACT CHANGED: _FailFastAdminClient no longer guards the wakeup busy loop")

    contract("kafka-python-ng is the pinned 2.2.3", kafka.__version__ == "2.2.3",
             f"installed {kafka.__version__}")
    method = getattr(KafkaAdminClient, "_send_request_to_node", None)
    contract("KafkaAdminClient._send_request_to_node exists", callable(method), "found" if callable(method) else "missing")
    params = list(inspect.signature(method).parameters) if callable(method) else None
    contract("its signature is (self, node_id, request, wakeup)",
             params == ["self", "node_id", "request", "wakeup"], f"{params}")
    sends = len(re.findall(r"self\._client\.send\(", inspect.getsource(KafkaAdminClient)))
    contract("no other KafkaAdminClient path calls self._client.send", sends == 1,
             f"{sends} call sites")
    guard = getattr(skc, "_FailFastAdminClient", None)
    overridden = guard is not None and guard._send_request_to_node is not method
    contract("_FailFastAdminClient overrides it", overridden,
             "overridden" if overridden else "missing or not overridden")

    b = FakeBroker()
    cid = f"127.0.0.1:{b.port}"
    admin, _lock = skc.get_shared_admin_client(cid, cfg(b.port))
    contract("get_shared_admin_client returns a _FailFastAdminClient",
             guard is not None and type(admin) is guard, f"got {type(admin).__name__}")
    client = admin._client
    contract("KafkaClient has _closed, False while open",
             "_closed" in vars(client) and client._closed is False, f"vars has _closed: {'_closed' in vars(client)}")
    skc.invalidate_client(cid)
    contract("KafkaClient._closed is True after close()", getattr(client, "_closed", None) is True,
             f"{getattr(client, '_closed', 'missing')}")


def test_healthy_client_still_works():
    b = FakeBroker()
    cid = f"127.0.0.1:{b.port}"
    admin, lock = skc.get_shared_admin_client(cid, cfg(b.port))
    t, outcome = use_in_thread(admin, lock, cid, "healthy")
    t.join(DEADLINE_SECS)
    check("healthy: list_consumer_groups works", outcome.get("value") == [("g1", "consumer")], repr(outcome))
    skc.invalidate_client(cid)


def test_refresh_race():
    """refresh_all_shared_clients closes a client a caller already fetched
    but has not locked yet (the window its docstring accepts as 'fail once')."""
    b = FakeBroker()
    cid = f"127.0.0.1:{b.port}"
    admin, lock = skc.get_shared_admin_client(cid, cfg(b.port))
    refreshed = skc.refresh_all_shared_clients()
    assert cid in refreshed["refreshed"], refreshed
    started = time.time()
    t, outcome = use_in_thread(admin, lock, cid, "race-caller")
    finished_with_connection_error("refresh race", t, outcome, started)


def test_closed_while_connecting():
    """invalidate_client (as acquire_admin_lock's timeout path calls it)
    closes the client while its lock holder waits for a broker connection."""
    b = FakeBroker()
    cid = f"127.0.0.1:{b.port}"
    admin, lock = skc.get_shared_admin_client(cid, cfg(b.port))
    add_unreachable_node(b, admin)
    started = time.time()
    t, outcome = use_in_thread(admin, lock, cid, "holder")
    time.sleep(1)
    assert t.is_alive(), "holder should still be waiting for node 1"
    skc.invalidate_client(cid)
    finished_with_connection_error("closed while connecting", t, outcome, started)


if __name__ == "__main__":
    test_library_contract()
    test_healthy_client_still_works()
    test_refresh_race()
    test_closed_while_connecting()
    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed; wakeup warnings logged in total: {len(WAKEUPS):,}", flush=True)
    os._exit(0 if all(RESULTS) else 1)
