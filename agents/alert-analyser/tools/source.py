import base64
import csv
import io
import json
from abc import ABC, abstractmethod
from datetime import datetime


class AlertSource(ABC):
    @abstractmethod
    async def load_alerts(self) -> list[dict]:
        ...


class FileSource(AlertSource):
    """Reads alert data from an uploaded CSV or JSON string (stored in DB as TEXT)."""

    def __init__(self, raw_data: str, fmt: str = "json") -> None:
        self._raw_data = raw_data
        self._fmt = fmt.lower()

    async def load_alerts(self) -> list[dict]:
        if self._fmt == "csv":
            reader = csv.DictReader(io.StringIO(self._raw_data))
            return [dict(row) for row in reader]
        return json.loads(self._raw_data)


def _map_alert(a: dict) -> dict:
    """Map a raw OpsGenie / JSM alert dict to the internal alert format.

    Field names are identical between JSM and standalone OpsGenie, so both
    source classes share this function.
    """
    created_at = a.get("createdAt", "")
    updated_at = a.get("updatedAt", "")
    status = a.get("status", "")

    close_time = 0
    if status == "closed" and created_at and updated_at:
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            close_time = max(0, int((updated - created).total_seconds()))
        except Exception:
            pass

    responders = a.get("responders", [])
    teams = [
        r.get("name", str(r)) if isinstance(r, dict) else str(r)
        for r in responders
    ]

    return {
        "id": a.get("id", ""),
        "alias": a.get("alias", a.get("id", "")),
        "message": a.get("message", ""),
        "status": status,
        "priority": a.get("priority", "P5"),
        "source": a.get("source", "unknown"),
        "createdAt": created_at,
        "acknowledged": a.get("acknowledged", False),
        "count": a.get("count", 1),
        "teams": teams[:1] or ["Unknown"],
        "report": {"closeTime": close_time},
    }


class JSMSource(AlertSource):
    """Atlassian JSM Ops API — api.atlassian.com/jsm/ops/api/.

    Auth:       HTTP Basic (email + API token)
    Pagination: offset-based via response links.next URL
    Time filter: createdAfter query parameter (ISO-8601)
    """

    def __init__(self, cloud_id: str, email: str, api_token: str) -> None:
        self._cloud_id = cloud_id
        self._email = email
        self._api_token = api_token

    async def load_alerts(
        self,
        sync_window_days: int = 7,
        created_after: str | None = None,
    ) -> list[dict]:
        import httpx
        from datetime import timedelta, timezone
        from urllib.parse import parse_qs, urlparse

        if created_after is not None:
            start = datetime.fromisoformat(created_after)
        else:
            start = datetime.now(timezone.utc) - timedelta(days=sync_window_days)

        created_after_iso = start.isoformat()

        all_alerts: list[dict] = []
        cursor: str | None = None
        url = f"https://api.atlassian.com/jsm/ops/api/{self._cloud_id}/v1/alerts"
        credentials = base64.b64encode(f"{self._email}:{self._api_token}".encode()).decode()
        headers = {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict = {"limit": 50, "createdAfter": created_after_iso}
                if cursor is not None:
                    params["offset"] = cursor

                for _attempt in range(3):
                    resp = await client.get(url, headers=headers, params=params)
                    if resp.status_code == 429:
                        import asyncio
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    break
                else:
                    resp.raise_for_status()
                data = resp.json()

                page_alerts = data.get("values", [])
                all_alerts.extend(page_alerts)

                links = data.get("links", {})
                next_url = links.get("next") if isinstance(links, dict) else None
                if not next_url:
                    break
                parsed = urlparse(next_url)
                offset_list = parse_qs(parsed.query).get("offset", [])
                if not offset_list:
                    break
                cursor = offset_list[0]

        return [_map_alert(a) for a in all_alerts]


class StandaloneOpsgenieSource(AlertSource):
    """Standalone OpsGenie API — api.opsgenie.com/v2/.

    Auth:       GenieKey header (API key from OpsGenie settings)
    Pagination: offset-based; continues while paging.next is present
    Time filter: Lucene query string (createdAt > epoch_ms)
    base_url:   Override for EU region (api.eu.opsgenie.com) or on-prem.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.opsgenie.com",
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")

    async def load_alerts(
        self,
        sync_window_days: int = 7,
        created_after: str | None = None,
    ) -> list[dict]:
        import httpx
        from datetime import timedelta, timezone

        if created_after is not None:
            start = datetime.fromisoformat(created_after)
        else:
            start = datetime.now(timezone.utc) - timedelta(days=sync_window_days)

        # Standalone OpsGenie query syntax uses epoch milliseconds
        epoch_ms = int(start.timestamp() * 1000)
        query = f"createdAt>{epoch_ms}"

        all_alerts: list[dict] = []
        limit = 100
        offset = 0
        url = f"{self._base_url}/v2/alerts"
        headers = {
            "Authorization": f"GenieKey {self._api_key}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict = {
                    "query": query,
                    "limit": limit,
                    "offset": offset,
                    "order": "asc",
                }

                for _attempt in range(3):
                    resp = await client.get(url, headers=headers, params=params)
                    if resp.status_code == 429:
                        import asyncio
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    break
                else:
                    resp.raise_for_status()
                data = resp.json()

                page_alerts = data.get("data", [])
                all_alerts.extend(page_alerts)

                paging = data.get("paging", {})
                has_next = bool(paging.get("next")) if isinstance(paging, dict) else False
                if len(page_alerts) < limit or not has_next:
                    break
                offset += limit

        return [_map_alert(a) for a in all_alerts]


# Backward-compatibility alias — existing callers (routes_settings.py, main.py) keep working.
OpsgenieAPISource = JSMSource
