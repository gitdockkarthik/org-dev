# Infrastructure — KPI Box Reference

Cross-agent infrastructure notes for the KPI box (10.51.2.101). Not agent-specific — see individual agents' BACKLOG.md files for agent-scoped work.

## Disks

Two physical NVMe devices:
- `/dev/nvme0n1p1` (30G) — mounted `/`, root filesystem
- `/dev/nvme1n1` (60G) — mounted `/data`, hosts all Docker volumes (including `pgdata`) and the org-dev repo checkout

## Postgres Data Durability

The `postgres` service uses a named Docker volume (`org-dev_pgdata`, physically at `/data/docker/volumes/org-dev_pgdata/_data`) — not container-local storage. This means:
- Container restarts, crashes, OOM kills, and image rebuilds do NOT affect data
- Data is only at risk from `docker compose down -v` (explicit volume removal) or a `/data` disk-level failure
- Confirmed safe across the mem_limit rollout (image rebuild) and multiple alert-analyser rebuilds during the 03/08/26 memory investigation

## Postgres Backup (added 04/08/26)

Since `pgdata` lives on `/data`, backups are stored on `/` (the other physical disk) for genuine disk-failure protection, not just container-lifecycle protection.

- **Script:** `/home/backups/postgres_backup.sh` — runs `pg_dump -F c` against `operative_db`, copies out via `docker compose cp`, rotates backups older than 3 days
- **Schedule:** systemd timer `postgres-backup.timer`, daily at 02:00 UTC, `Persistent=true` (catches up on missed runs after downtime/reboot) — same pattern as the existing `docker-housekeeping.timer`
- **Location:** `/home/backups/postgres/` (root disk, separate from `/data`)
- **Retention:** 3 days (dev-box appropriate; not production compliance retention)
- **Verify:** `systemctl list-timers --all | grep postgres-backup` and `ls -lh /home/backups/postgres/`
- **Manual trigger:** `systemctl start postgres-backup.service`
- **Logs:** `/home/backups/postgres_backup.log`, or `journalctl -u postgres-backup.service`

This covers the entire `operative_db` database — CUR reports, alert history, incidents, kafka data — not just alert-analyser's tables.

## Scheduled Jobs on This Box (systemd timers, no cron installed)

| Timer | Schedule | Purpose |
|---|---|---|
| `docker-housekeeping.timer` | Twice daily (06:00, 18:00 UTC) | Prune dangling images + unused build cache |
| `postgres-backup.timer` | Daily (02:00 UTC) | pg_dump backup of operative_db, 3-day rotation |

---
*Operative Intelligence — Internal Platforms Engineering. Last updated: 04/08/26.*
