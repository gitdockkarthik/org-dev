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


class OpsgenieAPISource(AlertSource):
    """Fetches live alerts from the Atlassian JSM Ops API and maps them to internal format."""

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

        if created_after is not None:
            start = datetime.fromisoformat(created_after)
        else:
            start = datetime.now(timezone.utc) - timedelta(days=sync_window_days)

        created_after_iso = start.isoformat()

        all_alerts: list[dict] = []
        cursor: str | None = None
        base_url = f"https://api.atlassian.com/jsm/ops/api/{self._cloud_id}/v1/alerts"
        credentials = base64.b64encode(f"{self._email}:{self._api_token}".encode()).decode()
        headers = {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict = {"limit": 50, "createdAfter": created_after_iso}
                if cursor is not None:
                    params["cursor"] = cursor

                resp = await client.get(base_url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()

                page_alerts = data.get("values", [])
                all_alerts.extend(page_alerts)

                next_cursor = data.get("next")
                if not next_cursor:
                    break
                cursor = next_cursor

        return [self._map(a) for a in all_alerts]

    def _map(self, a: dict) -> dict:
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
