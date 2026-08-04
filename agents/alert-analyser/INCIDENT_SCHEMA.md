# Incident Management — Live Schema Reference

Reference doc for RCA Agent and App Support Agent teams building against the shared `incident_management` schema on the KPI box Postgres instance. Alert Analyser owns schema evolution — any structural changes should go through this repo, not applied independently by consuming agents. This doc reflects the live database as verified 03/08/26, not any earlier design proposal.

## Connection

Since RCA/App Support agents run as separate Docker Compose projects (own repo, own network) on the same KPI box, connect via the host-mapped port, not the internal container port.
