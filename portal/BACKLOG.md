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
Status: IN PROGRESS — Chunks 1-4 DONE, Chunk 5a DONE (9f2c7e7): per-user bulk
access endpoints added — GET /api/registry/users/{email}/agent-access (returns
current agent_slugs) and PUT (atomic replace: deletes all existing AgentAccess
rows for that user, inserts new set). Admin-only. Validated live against
test.user@operative.com (set to 2 agents, confirmed, restored to original 4).
Design decision: per-user UI chosen over per-agent — admin picks one user, sees
multi-select of all published agents, saves in one action. Scales better than
per-agent as user count grows (33 users today).
UI location identified: portal/admin/index.html — existing Users admin table
(Name/Email/Role/Created/Actions columns, action buttons: Roles, Reset PW, Delete).
Chunk 5b: add new "Access" action button per row, opens modal with checkbox list
of all published agents, pre-populated via GET, saved via PUT. Starting next.
Plan:
  - [DONE] Chunk 1: AgentAccess model, alembic migration (schema only), commit.
  - [DONE] Chunk 2: backfill script (all users x all published agents), run once, commit.
  - [DONE] Chunk 3: filter list_agents() for plain `user` role only (admin/developer untouched).
  - [DONE] Chunk 4: admin-only endpoints GET/POST/DELETE /agents/{slug}/access (mirrors owners).
  - [DONE] Chunk 5a: per-user bulk access endpoints GET/PUT /users/{email}/agent-access.
  - Chunk 5b: portal UI — add "Access" action button to Users admin table, modal with checkbox list.
Each chunk: implement, validate (portal is primary validation surface per this file's
rule), commit, THEN move to next chunk.

---

## Resolved
