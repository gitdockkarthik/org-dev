# Backlog

**Rule: this file is the only source of truth for pending work.** If an item isn't here,
committed to git, it doesn't exist — regardless of what was said in any chat session.
Update this file in the SAME commit as the code change that creates, resolves, or modifies
an item. Never treat "I'll add it to the backlog" as done until it's in this file and
`git log` shows it committed.

**Rule: always run `git status` immediately before `git commit`, not just before `git add`.**

Each item: short description, why it matters, status, date added.

---

## Open

### Agent-level access control for Agents Catalogue page (2026-08-05)
Currently RBAC only restricts agent OWNERSHIP (AgentOwner — API key generation scope,
developer role). There is no restriction on which agents a plain `user` role can VIEW/USE
from the Portal catalogue (GET /api/registry/agents in backend/registry/router.py) —
all published agents are visible to every logged-in user regardless of role.
Adding a new, separate AgentAccess model (mirrors AgentOwner exactly, does NOT touch or
interact with AgentOwner/developer ownership) to scope catalogue visibility per-user.
Rollout plan: backfill ALL existing users with access to ALL published agents at
migration time, so no one sees a blank catalogue on deploy — access will then be
manually curated per-user by admin working with team managers.
Status: IN PROGRESS — Chunk 1 DONE (36836b8), Chunk 2 DONE (c1c119b),
Chunk 3 DONE (54d8495): catalogue filtering enforced and validated live
(test.user@operative.com confirmed sees 4/5 agents, mock-agent correctly hidden).
Key discovery during Chunk 3: the portal catalogue does NOT call
/api/registry/agents (backend/registry/router.py) as originally assumed — it calls
/api/agents (list_published_agents() in backend/orchestrator/router.py), a
previously PUBLIC, unauthenticated endpoint with no current_user resolution at all.
Applied the AgentAccess filter to BOTH endpoints for consistency:
  - /api/agents (orchestrator/router.py) — the one the portal actually uses.
    Now REQUIRES authentication (403 if no valid session) — this closes a prior
    public/unauthenticated bypass, since portal.js's fetchAgents() previously sent
    no credentials at all. Confirmed frontend already redirects to login on 403,
    so this is a safe tightening, not a breaking change.
  - /api/registry/agents (registry/router.py) — filtered too, for consistency,
    though not the portal's actual call path; used elsewhere (admin tooling).
Also fixed: portal/js/portal.js fetchAgents() now sends credentials: 'include'
(previously sent no cookie at all).
Chunk 4 (admin endpoints: GET/POST/DELETE /agents/{slug}/access, mirrors owners)
starting next.
Plan:
  - [DONE] Chunk 1: AgentAccess model, alembic migration (schema only), commit.
  - [DONE] Chunk 2: backfill script (all users x all published agents), run once, commit.
  - [DONE] Chunk 3: filter list_agents() for plain `user` role only (admin/developer untouched).
  - Chunk 4: admin-only endpoints GET/POST/DELETE /agents/{slug}/access (mirrors owners).
  - Chunk 5: portal UI (no precedent screen exists yet — owners has no UI either,
    API-only today; UI surface TBD, decide at that chunk).
Each chunk: implement, validate (portal is primary validation surface per this file's
rule), commit, THEN move to next chunk.

---

## Resolved
