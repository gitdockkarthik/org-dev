"""routes_settings.save_settings: broker_restart_window_minutes must be a whole number from 15 to 240
(HTTP 422 otherwise, nothing stored). The database write (_upsert) is replaced by a recorder.
Run inside the agent image: python -B tests/test_settings_broker_restart_window.py"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from fastapi import HTTPException  # noqa: E402

import routes_settings as rs  # noqa: E402

PASSED = []
SAVED = []


async def _upsert(key, value):
    SAVED.append((key, value))


rs._upsert = _upsert


class Req:
    def __init__(self, body):
        self.body = body

    async def json(self):
        return self.body


def check(name, cond, detail=""):
    assert cond, f"FAIL {name} {detail}"
    PASSED.append(name)
    print(f"PASS {name}")


async def save(value):
    SAVED.clear()
    try:
        return await rs.save_settings(Req({"broker_restart_window_minutes": value})), None
    except HTTPException as exc:
        return None, exc


async def main():
    for ok in (15, 30, 240):
        res, exc = await save(ok)
        check(f"{ok} accepted and stored", exc is None and res == {"ok": True} and SAVED == [("broker_restart_window_minutes", ok)] and rs._config["broker_restart_window_minutes"] == ok)
    before = rs._config["broker_restart_window_minutes"]
    for bad in (14, 241, 0, -30, 30.5, "30", None, True, [30]):
        res, exc = await save(bad)
        check(f"{bad!r} rejected with 422, nothing stored", exc is not None and exc.status_code == 422 and not SAVED
              and rs._config["broker_restart_window_minutes"] == before and "15 to 240" in exc.detail)
    res, exc = await rs.save_settings(Req({"teams_webhook_url": "x"})), None
    check("a save without the key is unaffected", res == {"ok": True} and ("teams_webhook_url", "x") in SAVED)
    print(f"\n{len(PASSED)} checks passed")


asyncio.run(main())
