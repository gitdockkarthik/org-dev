"""Broker restart capture, step 1: decide_restart and the scrape_broker parsing of
process_start_time_seconds. No database, network or curl: the curl is replaced
by canned text. Plain asserts, no pytest needed.
Run: python -B agents/kafka-analyser/tests/test_broker_restart_unit.py
(Python 3.10+ with the agent's requirements)."""
import asyncio
import os
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from broker_restarts import decide_restart  # noqa: E402
from tools import prometheus_collector as pc  # noqa: E402

PASSED = []


def check(name, cond, detail=""):
    assert cond, f"FAIL {name} {detail}"
    PASSED.append(name)
    print(f"PASS {name}")


T = 1.792092709056e9  # 2026-10-15 19:31:49 UTC, aos-prod-kafka02

# ── decide_restart ───────────────────────────────────────────────────────────
check("new None keeps stored, no event", decide_restart(T, None, 0) == (T, None, None))
check("both None", decide_restart(None, None, 0) == (None, None, None))
check("stored None stores the baseline, no event", decide_restart(None, T, 0) == (T, None, None))
v, ev, w = decide_restart(T, T + 6, 0)
check("6 s later is a restart", v == T + 6 and ev == {"started_at": T + 6, "previous_started_at": T} and w is None, (v, ev, w))
check("exactly 5 s later is not", decide_restart(T, T + 5, 0) == (T, None, None))
check("3 s later is jitter, stored kept", decide_restart(T, T + 3, 0) == (T, None, None))
check("3 s earlier is jitter, stored kept", decide_restart(T, T - 3, 0) == (T, None, None))
check("same value", decide_restart(T, T, 0) == (T, None, None))
v, ev, w = decide_restart(T, T - 6, 0)
check("6 s earlier keeps stored and warns", v == T and ev is None and "earlier" in w, (v, ev, w))
v, ev, w = decide_restart(T, T + 86400 * 13, 0)
check("13 days later is a restart", ev is not None and v == T + 86400 * 13)

# ── scrape_broker parsing (fake curl) ────────────────────────────────────────
SAVED = []


async def _noop_save(key, state):
    SAVED.append(key)


pc._save_scrape_state = _noop_save


def scrape(body):
    async def fake_curl(url, max_time=10):
        assert "name[]=process_start_time_seconds" in url
        return body
    pc._curl_get = fake_curl
    pc._broker_state.clear()
    return asyncio.run(pc.scrape_broker("b1", 7071))


BASE = "process_cpu_seconds_total 12.5\njvm_memory_bytes_used{area=\"heap\"} 50\njvm_memory_bytes_max{area=\"heap\"} 100\n"
check("filter list has the metric", "process_start_time_seconds" in pc._FILTERED_METRICS)
r = scrape(BASE + "# HELP process_start_time_seconds Start time\n# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds 1.792092709056E9\n")
check("metric present parsed", r["process_start_time"] == 1.792092709056e9 and r["heap_pct"] == 50.0, r)
r = scrape(BASE)
check("metric absent is None", r["process_start_time"] is None and r["heap_pct"] == 50.0, r)
r = scrape("")
check("failed curl is None, other fields as before", r["process_start_time"] is None and r["heap_pct"] == 0.0, r)
r = scrape("<html>502 Bad Gateway</html>\nprocess_start_time_seconds garbage\nprocess_start_time_seconds\n")
check("garbage is None", r["process_start_time"] is None, r)
for label, val in (("0.0", "0.0"), ("below 1e9", "999999999"), ("negative", "-5"), ("1e9 exactly", "1e9")):
    r = scrape(BASE + f"process_start_time_seconds {val}\n")
    check(f"value {label} is None, never 0.0", r["process_start_time"] is None, r)
r = scrape(BASE + "process_start_time_seconds{instance=\"x\"} 1.7e9\n")
check("labelled sample accepted", r["process_start_time"] == 1.7e9, r)


async def _boom(*a, **k):
    raise RuntimeError("boom")


pc._curl_get = _boom
r = asyncio.run(pc.scrape_broker("b1", 7071))
check("exception path returns defaults with None", r["process_start_time"] is None and r["throughput_available"] is False, r)

print(f"\n{len(PASSED)} checks passed")
