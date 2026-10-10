"""The startup warm-up must connect to each cluster the way the shared
AdminClient does. Before the fix main.py's _warmup_connections built its
KafkaAdminClient with only bootstrap_servers and api_version, so a
sasl_scram + TLS cluster (cluster 4) got a plaintext connection to its
SASL_SSL listener and every restart logged "socket disconnected" and
"Warm-up connection failed ... UnrecognizedBrokerVersion".

Runs main.py's real _warmup_connections body (extracted with ast, so the
FastAPI app is not imported) against a stub storage backend:
  1. a stub KafkaAdminClient records constructor kwargs: the sasl_scram + TLS
     cluster gets SASL_SSL, its mechanism, credentials and an ssl_context; the
     plain cluster gets no security kwargs; both match the shared client's
     admin_client_kwargs apart from the warm-up's 10 s request timeout;
  2. against a local fake broker (_fake_kafka_broker.py, plaintext): the plain
     cluster connects, the SASL_SSL one fails with exactly one WARNING naming
     the cluster, no traceback, and no credential in any log line at the
     app's level (INFO, main.py's basicConfig), from any logger. Below that,
     kafka-python-ng's own KafkaAdminClient.__init__ logs its whole config,
     password included, at DEBUG -- for every admin client, not just this
     one -- so the test prints a NOTE if it sees that, rather than failing.
No real cluster, no database. Plain asserts, no pytest needed. Run inside the
agent image with the agent dir on PYTHONPATH, e.g.:
  docker run --rm --network none -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -w /app org-dev-kafka-analyser python -B tests/test_warmup_security.py
Exits 0 when every check passes, 1 otherwise."""
import ast
import asyncio
import logging
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
AGENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, AGENT_DIR)

from _fake_kafka_broker import FakeBroker  # noqa: E402
import shared_kafka_clients as skc  # noqa: E402

PASSWORD = "s3cr3t-Pa55w0rd-xyz"
USERNAME = "scram-user-q7"
RESULTS = []
LOG_LINES = []


class _Capture(logging.Handler):
    def emit(self, record):
        LOG_LINES.append((record.levelno, record.name, self.format(record)))


_handler = _Capture()
_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
logging.getLogger().addHandler(_handler)
logging.getLogger().setLevel(logging.DEBUG)


def check(name, cond, detail=""):
    RESULTS.append(bool(cond))
    print(f"{'PASS' if cond else 'FAIL'} {name}{': ' + detail if detail else ''}", flush=True)


def clusters(plain_port, scram_port):
    # Shaped like PostgresBackend._row_to_dict (sasl_password already decrypted).
    return [
        {"id": 1, "name": "plain-cluster", "enabled": True, "bootstrap_servers": f"127.0.0.1:{plain_port}",
         "auth_type": "none", "sasl_username": None, "sasl_password": None,
         "sasl_mechanism": None, "tls_enabled": False},
        {"id": 4, "name": "scram-tls-cluster", "enabled": True, "bootstrap_servers": f"127.0.0.1:{scram_port}",
         "auth_type": "sasl_scram", "sasl_username": USERNAME, "sasl_password": PASSWORD,
         "sasl_mechanism": "SCRAM-SHA-512", "tls_enabled": True},
    ]


def load_warmup(cluster_rows):
    """main.py's nested _warmup_connections, compiled on its own with stub
    storage/settings, and with its 5 s start delay kept as written."""
    tree = ast.parse(open(os.path.join(AGENT_DIR, "main.py")).read())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_warmup_connections")
    module = ast.Module(body=[fn], type_ignores=[])

    class _Backend:
        async def get_clusters(self, slug):
            return [dict(c) for c in cluster_rows]

    sys.modules["storage"] = types.SimpleNamespace(get_backend=lambda: _Backend())
    ns = {"asyncio": asyncio, "logger": logging.getLogger("main"),
          "settings": types.SimpleNamespace(agent_slug="kafka-analyser")}
    exec(compile(module, "main.py", "exec"), ns)
    return ns["_warmup_connections"]


def test_stub_kwargs():
    built = []

    class _StubAdmin:
        def __init__(self, **kwargs):
            built.append(kwargs)

        def close(self):
            pass

    real = skc.KafkaAdminClient
    skc.KafkaAdminClient = _StubAdmin
    try:
        rows = clusters(9001, 9004)
        asyncio.run(load_warmup(rows)())
    finally:
        skc.KafkaAdminClient = real
    by_bs = {k["bootstrap_servers"]: k for k in built}
    plain, scram = by_bs.get("127.0.0.1:9001", {}), by_bs.get("127.0.0.1:9004", {})
    check("stub: one client per cluster", len(built) == 2, str(len(built)))
    check("stub: plain cluster has no security kwargs",
          not any(k in plain for k in ("security_protocol", "sasl_mechanism", "sasl_plain_username",
                                       "sasl_plain_password", "ssl_context")), str(sorted(plain)))
    check("stub: scram+TLS cluster uses SASL_SSL", scram.get("security_protocol") == "SASL_SSL")
    check("stub: scram+TLS cluster mechanism", scram.get("sasl_mechanism") == "SCRAM-SHA-512")
    check("stub: scram+TLS cluster credentials",
          scram.get("sasl_plain_username") == USERNAME and scram.get("sasl_plain_password") == PASSWORD)
    check("stub: scram+TLS cluster ssl_context", scram.get("ssl_context") is not None)
    for name, got, row in (("plain", plain, rows[0]), ("scram+TLS", scram, rows[1])):
        want = skc.admin_client_kwargs({**row, "sasl_mechanism": row["sasl_mechanism"] or "PLAIN"})
        same = {k: v for k, v in got.items() if k not in ("ssl_context", "request_timeout_ms")} == \
               {k: v for k, v in want.items() if k not in ("ssl_context", "request_timeout_ms")}
        check(f"stub: {name} kwargs match the shared client's", same)
        check(f"stub: {name} keeps the 10 s timeout and version probe bound",
              got.get("request_timeout_ms") == 10000 and got.get("api_version") == (2, 3, 0)
              and got.get("api_version_auto_timeout_ms") == 2000)


def test_fake_broker_logs():
    plain, scram = FakeBroker(), FakeBroker()
    LOG_LINES.clear()
    asyncio.run(load_warmup(clusters(plain.port, scram.port))())
    warnings = [m for lvl, n, m in LOG_LINES if lvl >= logging.WARNING and n == "main"]
    oks = [m for lvl, n, m in LOG_LINES if n == "main" and "Warm-up connection OK" in m]
    check("broker: plain cluster connects", any("plain-cluster" in m for m in oks), str(oks))
    check("broker: plain cluster got plaintext ApiVersions", any(r[1] == 18 for r in plain.requests))
    check("broker: one warm-up WARNING, for the scram+TLS cluster",
          len(warnings) == 1 and "scram-tls-cluster" in warnings[0], str(warnings))
    check("broker: scram+TLS cluster was not sent a plaintext Kafka request",
          not scram.requests, str(scram.requests))
    check("broker: warning is one short line, no traceback",
          bool(warnings) and "\n" not in warnings[0] and "Traceback" not in warnings[0]
          and len(warnings[0]) < 400, warnings[0] if warnings else "")
    shown = [(lvl, n, m) for lvl, n, m in LOG_LINES if lvl >= logging.INFO]
    leaked = [m for _, _, m in shown if PASSWORD in m or USERNAME in m]
    check(f"broker: no credential in any of {len(shown)} INFO+ log lines", not leaked, str(leaked[:2]))
    debug_leaks = {n for lvl, n, m in LOG_LINES if lvl < logging.INFO and PASSWORD in m}
    if debug_leaks:
        print(f"NOTE library DEBUG logging includes the password (loggers: {sorted(debug_leaks)}); "
              "not emitted at the app's INFO level", flush=True)


def test_reason_redacts():
    row = clusters(1, 2)[1]
    r = skc.safe_failure_reason(RuntimeError(f"auth failed for {USERNAME} with {PASSWORD}\nmore"), row)
    check("reason: credentials redacted, first line only",
          PASSWORD not in r and USERNAME not in r and "more" not in r, r)


if __name__ == "__main__":
    test_stub_kwargs()
    test_fake_broker_logs()
    test_reason_redacts()
    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed", flush=True)
    os._exit(0 if all(RESULTS) else 1)
