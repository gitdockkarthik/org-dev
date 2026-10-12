# Page stubs (teams.html, settings.html)

Each script loads the real portal page
(`portal/agents/kafka-analyser/teams.html`, some also `settings.html`) in jsdom,
runs its inline scripts with `fetch` stubbed, and prints one `[PASS]`/`[FAIL]`
(or `PASS`/`FAIL`) line per check. No backend, database or browser is needed.
jsdom has no layout engine, so overlaps and spacing still need a look in a browser.

## Run

Needs node (18+) and jsdom 24. Install jsdom outside the repo and point `NODE_PATH` at it:

    npm install --prefix /tmp/jsdom24 jsdom@24
    export NODE_PATH=/tmp/jsdom24/node_modules

All stubs, with PASS/FAIL counts (exits 1 if any check fails):

    agents/kafka-analyser/tests/pages/run_all.sh

One stub:

    node agents/kafka-analyser/tests/pages/form_slo.js

To test another copy of teams.html: `TEAMS_HTML=/path/to/teams.html run_all.sh`.

## Files

- `_page.js`: shared helpers. Paths are relative to the repo. Expectations
  about alert types come from the page's own `ALERT_TYPES`: the Cluster group
  matches `ALERT_TYPES` in order, and position checks look for a consecutive
  run instead of "last in the list". `tierHintProblems` checks every type:
  - the above hint shows only for types without `below`;
  - every `below` type shows exactly one other hint;
  - no tier hint shows for more than one type;
  - the hint owners a stub names are exact.

  So adding a type should not break these checks, as long as the new type
  follows the rules.
- `configs_*.js`: alert rules table (filters, paging, layout, enabled and email
  toggles, copy panel).
- `form_*.js`: the alert rule form, one file per type family (urp_rf, msg_rate,
  data, leader_skew, zk, connect, schema_registry, lag, slo), plus the cluster
  select, "Create for every cluster" and the tier layout.
- `form_tier_layout.js`: also checks that every element id in the committed
  (git HEAD) teams.html still exists.
- `triggers.js`: Recent Triggers table (30-day window, filters, paging).
- `settings_teams_step3*.js`: email settings and email toggle stubs for both
  pages. Step 3c writes one temporary copy of settings.html under the OS temp
  directory and deletes it afterwards.
