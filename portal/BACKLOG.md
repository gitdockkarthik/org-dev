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

---

## Resolved

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
Status: DONE — all chunks shipped and validated live.
  Chunk 1 (model + migration): 36836b8
  Chunk 2 (backfill): c1c119b
  Chunk 3 (catalogue filter enforcement): 54d8495
  Chunk 4 (per-agent admin endpoints): f737e89, 5dc7943
  Chunk 5a (per-user bulk access endpoints): 9f2c7e7
  Chunk 5b (Admin Users UI — Access button + modal): f153119

Feature summary: Plain `user` role now only sees agents explicitly granted via the
agent_access table on the Agents Catalogue page. Admin manages this per-user via
Admin > Users > Access button — checkbox list of all published agents, saved via
atomic PUT. All 33 existing users were backfilled with access to all 5 published
agents EXCEPT mock-agent (deliberately withheld from all but
karthikeyan.gopalan@operative.com as a live validation control — mock-agent's
docker service is stopped, kept published in DB only for this purpose).
Admin and developer roles are unaffected by this filter — developer's existing
AgentOwner-based ownership/API-key scoping is completely separate and untouched.

Next step (not yet scheduled): decide whether to remove mock-agent from the
catalogue entirely now that validation is complete, or leave it as a permanent
test fixture for future access-control changes.
