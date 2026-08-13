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

### Root filesystem at 78% — stale VS Code Remote-SSH server caches, automated cleanup added (2026-08-13)
Root filesystem (/, 30G) reached 78% used. Investigation found this was
unrelated to Docker (Docker's data-root correctly lives on the separate
/data volume, healthy at 34%) — root's usage was driven by
~/.vscode-server/cli/servers/ accumulating one full server-version folder
(~600-700MB each) per VS Code update, across 3 user home directories
(karthikeyan.gopalan, praveen.kd, ec2-user), never cleaned up automatically.
Manually cleaned all stale versions (kept only the most-recently-used, per
each user's own lru.json) — freed ~7.5GB total, root dropped from 78% to 53%.

Added permanent fix: /usr/local/bin/vscode-server-cleanup.sh (root cron-style
job) scans all /home/*/.vscode-server/cli/servers/lru.json files, keeps only
the MRU entry, removes stale Stable-<hash> folders and orphaned top-level
code-<hash> binaries, logs to /var/log/vscode-server-cleanup.log. Installed
as systemd service + timer (vscode-server-cleanup.service/.timer, matches
the existing docker-housekeeping.service/.timer pattern already on this
box), runs daily at 05:00 UTC, Persistent=true. Validated live: manual run
found and removed an additional 1.5GB of stale data missed by the initial
manual pass, confirming the automated logic works correctly and will
prevent this from silently reaccumulating.

### Fix: uses_uap_llm flag was cosmetic only, never enforced (2026-08-10, commit 8441223)
Discovered while discussing what "checking Uses UAP LLM at registration" should
actually mean. The flag existed on the Agent model and was shown in the
catalogue/registry API, but /api/llm/token, /api/llm/invoke, and
/api/llm/invoke/stream never checked it — any agent with a valid, correctly-
scoped developer key could use the Gateway regardless of this flag's value.
This meant rca-agent and radar successfully used the Gateway earlier the same
day while their uses_uap_llm was still false — proving the flag was purely
cosmetic.

Fixed: all three endpoints now look up the Agent record and reject with 403
("Agent '<slug>' is not registered to use the UAP LLM Gateway...") if
uses_uap_llm is false, even with a valid key. This is the intended contract:
the flag tells the platform whether to serve LLM calls for that agent; if
false, the agent must fall back to its own direct LLM credentials.

As part of this fix, deduplicated a redundant Agent lookup that existed in
both llm_invoke() and llm_invoke_stream() (each previously queried Agent
twice — once implicitly needed for this new check, once later for the
app_label/Langfuse attribution lookup); now queried once and reused.

Flipped uses_uap_llm=true (via Admin UI) for rca-agent, radar, and
app-support-v2 — all three already validated using the Gateway before this
enforcement went live, so their access continues uninterrupted. Native agents
(alert-analyser, cur-analyser, kafka-analyser) remain false, correctly, since
they don't yet call the Gateway at all (in-process create_message() only) —
each will be flipped individually as part of its own migration chunk.

Validated: mock-agent (uses_uap_llm=true) unaffected; rca-agent blocked with
new 403 while flag was false, then confirmed working again immediately after
flipping to true.

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

---

## Established Patterns

### Firewall/port exposure policy for standalone agent onboarding
Original design: only port 3000 (portal) open for inbound external access —
all internal service-to-service traffic (backend, native agents) stays on the
Docker Compose internal network, never exposed externally.
As standalone agents were onboarded (app-support-v2, mock-agent, rca-agent),
additional ports appear to have been opened externally over time on an
ad-hoc basis — this is a drift from the original model and a security
concern, not an intended pattern.
Correct model going forward: standalone agents call UAP exclusively through
port 3000 (portal's nginx, which already proxies all /api/* to backend
internally — confirmed working, see LLM Gateway fix entry above). The ONLY
ports that should be open for inbound external access are:
  - 3000 (portal, always)
  - Each standalone agent's OWN base + landing-page ports (e.g. rca-agent's
    8880/9990), since those are the agent's own service being reached
    directly by its users/UAP for health checks and its own UI — this is
    unavoidable and by design, not a gap.
Backend (8010) and native agents (8001/8002/8003) should NOT be externally
reachable — only reachable from other containers on the internal Docker
network, or from the KPI box's own localhost for admin/debugging.
Action: audit currently-open external ports against this list with
Amrithanshu/CloudOps and close anything that doesn't belong (tracked
separately, not in this session).

---

## Value-Add Ideas

### Migrate agents onto LLM Gateway + relabel Langfuse "User" as "Initiator"
Architecture decision (2026-08-07): ALL agents become self-contained Gateway
clients with automatic fallback to direct Anthropic/Bedrock credentials if
Gateway is unreachable. Full tool-use and streaming parity required — this is
not a reduced-functionality migration.

Broader goal: Migrate alert-analyser, cur-analyser, kafka-analyser off their
current direct in-process create_message() calls (using local langfuse dependency)
onto the same /api/llm/token + /api/llm/invoke Gateway pattern already working for
standalone agents (rca-agent, etc.). This removes their local langfuse dependency
entirely, fully centralizing tracing/cost tracking in backend (matching the
"Application: <name>" labeling already built for standalone calls) and removes
drift risk between two different tracing code paths. Once complete, relabel
Langfuse dashboard "User" column to "Initiator" (displays either real user email
or "Application: <Agent Name>").

Status: IN PROGRESS

Chunk 1 DONE (commit a7f172d): /api/llm/invoke now accepts an optional "tools"
array, passes it through to create_message(), and returns full response shape
("content" blocks + "stop_reason") alongside the existing "response" text field
for backward compatibility. Validated: no-tools call unchanged (mock-agent),
tool_use call returns correct content blocks with id/name/input (mock-agent +
synthetic get_weather tool test), cross-agent key scoping still enforced (403
unchanged).

Chunk 2 DONE (commit f85f91e): Added /api/llm/invoke/stream — SSE streaming
endpoint mirroring alert-analyser's existing stream_insights() wire format
exactly (data: <chunk>\n\n frames, [STOP_REASON] marker, [DONE] terminator) —
so once alert-analyser migrates, its existing SSE parsing logic needs zero
changes, only the call site swaps from in-process stream_message() to this
HTTP endpoint.

Bug found and fixed during Chunk 2 validation: hasattr(b, "text") incorrectly
matched Claude's "thinking" content blocks (which have a text attribute equal
to None), causing the "response" field in /api/llm/invoke to return null
instead of the actual answer whenever a thinking block preceded the text
block. Fixed in 3 places (llm_invoke(), llm_invoke_stream(), and
_lf_trace()) using getattr(b, "text", None) instead. This was a pre-existing
bug, not introduced by this session's changes — caught only because Chunk 2's
validation exercised a model response that included a thinking block.

Added user_id parameter to stream_message()'s signature (was missing,
unlike create_message()) so streaming calls get correct "Application: <name>"
attribution in Langfuse, matching non-streaming calls.

Validated: SSE streaming output correct end-to-end, non-streaming response
field fix confirmed, cross-agent scoping (403) re-confirmed on both endpoints
after rebuild, tools passthrough unaffected by these fixes.

Chunk 3 DONE (commits b5fce4e, c58a0e1): shared/llm.py now implements true
Gateway-first-with-fallback for BOTH create_message() and stream_message() —
zero code changes required in any agent. Resolution is entirely env-driven:
UAP_URL + UAP_AGENT_KEY + AGENT_SLUG (all already set per-agent in
docker-compose.yml today) determine whether the Gateway path is attempted;
any failure (network, auth, timeout, malformed response) logs a warning and
falls through transparently to the existing direct-provider logic, completely
unchanged.

Response-shape parity solved via shim classes (_GatewayResponse,
_GatewayContentBlock, _GatewayUsage) exposing the identical attribute
interface as the Anthropic SDK's Message object (.content, .stop_reason,
.usage.input_tokens/.output_tokens, and per-block .type/.text/.id/.name/.input)
— calling code (agent.py's tool-use loop) cannot tell which path served the
request.

Streaming's tool-use case (the hardest part): the Gateway's SSE stream only
carries text deltas + a final stop_reason, not structured tool_use blocks —
so when a stream ends in tool_use, _gateway_stream_message() makes one
non-streaming follow-up call to retrieve the actual blocks, executes tools
locally via the existing tool_executor callback (same contract as
_stream_with_tools()), and continues the Gateway conversation. This mirrors
the existing direct-provider streaming loop's structure exactly.

Token handling: no caching — every create_message()/stream_message() call is
fully self-contained (mint token, use it, discard). This is a deliberate
architectural choice, not an oversight: the 5-minute token is a
platform-enforced authorization window, not a credential the calling code
should manage or extend the lifetime of. Confirmed this doesn't conflict with
the separate guidance already given to standalone-agent teams who build their
own direct Gateway clients (e.g. RCA) — those teams still choose their own
caching strategy for their own bespoke client code; this only governs the
shared/llm.py helper path.

Validated live (inside alert-analyser's own container, using rca-agent's
credentials as a safe test harness — no changes made to alert-analyser's
actual runtime config): non-streaming Gateway success, non-streaming
fallback (invalid key), streaming Gateway success, streaming fallback,
and streaming WITH tool-use (tool executed correctly mid-conversation,
correct final answer, proper termination) — all 5 cases pass.

Architecture note (2026-08-10): dropped "native vs. standalone" framing per
correction — there is no such distinction in the intended design. Every
agent is a standalone unit in the agent runtime layer; alert-analyser,
cur-analyser, and kafka-analyser are simply out of conformance with that
one contract today (using an older in-process/backend-injected-key pattern),
not a different category of agent. This migration brings them into
conformance, it does not create a new pattern for them.

alert-analyser migration DONE (commit 77c498a) — first agent brought into
conformance with the standalone-agent-runtime contract. Purely config-driven
per the Chunk 3 design: added UAP_URL=http://backend:8000 and UAP_AGENT_KEY
(scoped developer key id=15) to its docker-compose.yml environment block,
flipped uses_uap_llm=true via Admin UI. Zero changes to its own LLM-calling
logic — agent.py's create_message() call site is completely untouched;
shared/llm.py transparently routes it through the Gateway.

Two real bugs found and fixed during this migration's validation (not
introduced by the migration itself — pre-existing, only surfaced because
this was the first time a real tool-use-based agent exercised these paths):

1. agents/alert-analyser/agent.py had the same hasattr(b, "text")-matches-
   thinking-blocks bug already fixed in backend/orchestrator/router.py
   earlier today — fixed identically (getattr(b, "text", None)).

2. Double-tracing: create_message()'s and stream_message()'s Gateway
   branches were calling _lf_trace() client-side (inside the calling agent's
   own process) IN ADDITION to backend's own correct server-side trace
   inside /llm/invoke — meaning every Gateway-routed LLM call was logged
   twice in Langfuse, once correctly labeled and once with a blank
   "Application:" label (explains earlier confusion about blank User
   rows — not a separate bug, same root cause). Fixed by removing the
   redundant client-side _lf_trace() calls entirely; Gateway calls are
   now traced exactly once, server-side only, matching direct-provider
   calls which are still correctly traced client-side (since those really
   do execute the LLM call in the agent's own process).

Validated end-to-end: real chat via /invoke exercising the full tool-use
loop (config lookups, classify_alerts-grounded answers with real percentages
from live data), streaming via /invoke/stream with correct SSE framing,
single correctly-labeled Langfuse trace per call confirmed across 3
independent test calls post-fix.

cur-analyser migration DONE (commit 5ed89ca) — config-only (UAP_URL/
UAP_AGENT_KEY id=16, uses_uap_llm=true). Two additional real bugs found and
fixed during validation, both structural (benefit every future Gateway-routed
agent, not just cur-analyser):
1. _GatewayContentBlock was a plain object, not JSON-serializable — broke
   tool-use conversation continuation the moment a Gateway response's content
   got re-appended into messages and sent back through create_message()
   (TypeError: not JSON serializable). Fixed by making it a dict subclass.
2. Extended-thinking blocks were serialized without their required
   thinking/signature fields — Anthropic's real thinking blocks always have
   thinking='' (empty string, not absent) plus a real signature; the original
   getattr(b,'thinking',None) truthiness check incorrectly treated the
   always-empty field as absent, silently dropping signature and causing
   'messages.N.content.0.thinking.thinking: Field required' on the next
   turn. Fixed by checking b.type=='thinking' explicitly.
Validated: real chat against real CUR cost data ($623K total, correct
service/tax/credit breakdown), streaming, 7 correctly-labeled single traces.

kafka-analyser migration DONE (commit 4a41d60) — config-only, ZERO code
fixes needed (inherited every fix already made for alert/cur). Validated:
real chat against a live staging Kafka cluster (real broker CPU/heap metrics,
325 consumer groups, specific lag/timeout diagnostics, self-reported tool
errors, chart payload — 96,965 tokens across the full conversation),
streaming, 3 correctly-labeled single traces.

MIGRATION COMPLETE — all agents (alert-analyser, cur-analyser, kafka-analyser,
app-support-v2, mock-agent, rca-agent, radar) are now architecturally
conformant: every agent in the runtime layer uses the identical Gateway-
first-with-fallback contract via shared/llm.py, with zero bespoke per-agent
LLM-calling code. The "native vs standalone" distinction that existed at the
start of this initiative no longer exists anywhere in the codebase.

DONE (commit eccf151): relabeled LLM Usage dashboard's "User" column and
filter dropdown to "Initiator" in portal/admin/audit.html — correctly
reflects that this field shows either a real user's email (portal chat) or
"Application: <Agent Name>" (Gateway service call). Audit-log table's
separate "User" column (unrelated, different table) left unchanged.
