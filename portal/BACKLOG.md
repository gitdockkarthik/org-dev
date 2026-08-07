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

Follow-up (2026-08-05, commit 6e1b0a2): Applied team-manager-confirmed curated
access for 28 users via backend/scripts/apply_agent_access_assignments.py
(one-off, not re-runnable safely without updating the ASSIGNMENTS dict first).
Also onboarded new agent rca-agent (standalone, invoke_url
http://kpi-internal.cloud.operative.com:8880, landing_page_url
http://kpi-internal.cloud.operative.com:9990/, uses_uap_llm=false pending LLM
Gateway integration) — registered and published via Admin UI, access granted
to team-confirmed users only in the same script run. Remaining users not in the
curated list retain their original backfilled access (all agents except
mock-agent) unless changed here.

### Fix: DeveloperKey.key_id AttributeError on API key create/rotate (2026-08-07, commit 284ee09)
POST /api/developer/keys (and rotate) threw 500 Internal Server Error —
AttributeError: 'DeveloperKey' object has no attribute 'key_id'. Root cause:
audit-log calls in backend/routes_developer_keys.py referenced a non-existent
key.key_id attribute (model's actual PK field is `id`); key_name in audit details
also incorrectly referenced key.key_id instead of key.label.
Surfaced when two newly-onboarded developers (vignesh.c@operative.com,
balasubramanian.p@operative.com — added as owners of newly-onboarded rca-agent)
tried generating their first API keys — first real use of the developer/owner
API-key flow since rca-agent onboarding.
Fixed: resource_id=str(key.id) (both create and rotate handlers),
details key_name=key.label. Validated via curl (create + delete) before rollout,
then confirmed working live by both affected developers.

### Fix: Langfuse tracing broken for LLM Gateway calls + developer-key scoping gap (2026-08-07, commits ef05e48, 535b24f)
Discovered while onboarding rca-agent's first LLM Gateway call:

1. Gap 1 (ef05e48): /api/llm/token and /api/llm/invoke only accepted the shared
   BACKEND_API_KEY (via require_api_key), despite documentation stating scoped
   Option 3 developer keys (opk_...) should also work. _check_developer_key()
   existed and worked correctly for /api/invoke/{slug}, but was never wired into
   the LLM Gateway endpoints. Fixed by calling _check_developer_key() first in
   both endpoints, falling through to shared key only if no valid developer key
   present. Validated: same-agent key succeeds, cross-agent key correctly 403s.

2. Gap 2 (535b24f): langfuse Python package was present in all agent
   requirements.txt (alert/cur/kafka/template) but MISSING from
   backend/requirements.txt — meaning ALL LLM Gateway calls (the actual call
   path for standalone agents like rca-agent, app-support-v2, mock-agent) were
   silently untraced since Langfuse client init failed at import time, caught
   by a broad except Exception. Added langfuse>=2.0.0 to backend/requirements.txt,
   rebuilt image.

3. Gap 3 (535b24f): even after tracing worked, _lf_trace() named traces using
   os.environ.get("AGENT_SLUG", "unknown") — the BACKEND container's own env
   var (unset), not the calling agent's actual slug. All Gateway-path traces
   showed as "unknown.llm_call". Added agent_slug_override parameter threaded
   through create_message()/stream_message()/_lf_trace(), passed explicitly
   as the token's parsed slug in /llm/invoke.

4. Gap 4 (535b24f): traces showed blank "User" for service-to-service Gateway
   calls (no logged-in human in this context, unlike portal-driven
   /invoke/{slug} chat which correctly shows the real user's email). Added a
   friendly "Application: <Agent Name>" label as user_id specifically for
   /llm/invoke calls, leaving native agents' portal-driven user attribution
   unchanged.

Architecture decision confirmed: tracing/cost-attribution responsibility is
centralized in the backend for all STANDALONE agents (app-support-v2,
mock-agent, rca-agent, and future onboards) — they never need their own
langfuse dependency or credentials. Native agents (alert/cur/kafka) still
trace client-side today; migrating them onto the same Gateway + centralized
tracing pattern (removing their local langfuse dependency) is flagged as
future scope, not yet started.

Process note: multiple edits in this session were shown as diffs by Claude Code
but not actually written to disk, discovered only via git diff / inspect.signature
verification after rebuilds silently failed to reflect the change. Reinforces
existing discipline: always verify with git diff on host before rebuilding,
not just trust a displayed diff summary.

Validated end-to-end via curl against mock-agent's key: token vend → invoke →
Langfuse trace showing Agent: mock-agent.llm_call, User: Application: Mock Agent,
correct token/cost figures. Cross-agent key rejection (403) reconfirmed post-fix.

Docs Hub (portal/docs-hub/agent-registration-demo.html) Option 2 section updated
with working curl examples and scoped-key recommendation for the RCA team and
future standalone agent onboarding.

### Fix: Docs Hub UAP_URL guidance exposed wrong port for external agent teams (2026-08-07, commit a15a459)
Discovered while helping rca-agent team debug a 404 on their first LLM Gateway
call. Docs Hub's Option 2 example showed a generic placeholder
(https://<uap-platform-url>) with no port, leading naturally to guessing the
backend's own port (8010) — which works for internal/VPN testing but is the
WRONG production pattern: exposing 8010 externally would bypass the intended
single-entry-point security model (only portal's nginx on port 3000 should be
externally reachable, matching CLAUDE.md's architecture).
Verified portal/nginx.conf already proxies all /api/* (including /api/llm/token,
/api/llm/invoke) to backend:8000 internally — confirmed via curl from both
inside the KPI box and externally over VPN using port 3000. No new proxy
config needed; the capability already existed, just undocumented correctly.
Fixed: Docs Hub now shows UAP_URL=http://kpi-internal.cloud.operative.com:3000
with an inline comment warning against using :8010 externally.
Action item: communicate to any team that may have already configured :8010
directly (informal/early testing) to switch to :3000 before their agent goes
to production, and confirm :8010/8001/8002/8003 are firewalled from external
access at the infra level (not yet verified — flagged for follow-up with
Amrithanshu/CloudOps).
