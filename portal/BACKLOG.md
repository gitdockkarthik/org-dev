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
Status: IN PROGRESS — Chunk 1 (model + migration) starting now.
Plan:
  - Chunk 1: AgentAccess model, alembic migration (schema only), commit.
  - Chunk 2: backfill script (all users x all published agents), run once, commit.
  - Chunk 3: filter list_agents() for plain `user` role only (admin/developer untouched).
  - Chunk 4: admin-only endpoints GET/POST/DELETE /agents/{slug}/access (mirrors owners).
  - Chunk 5: portal UI (no precedent screen exists yet — owners has no UI either,
    API-only today; UI surface TBD, decide at that chunk).
Each chunk: implement, validate (portal is primary validation surface per this file's
rule), commit, THEN move to next chunk.

---

## Resolved
