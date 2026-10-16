# apply_rule_plan.py: apply the Kafka head's thresholds

Takes his edited copy of the Teams tab's **Export CSV** (23 columns), compares
it with the live rules, and switches every planned rule to **Email on, Teams
off**. Rule 5 gets **Email on, Teams on**. His tier changes are applied at the
same time. It also sets the global `email_enabled` and `email_recipients`.

The default is a dry run. Nothing changes without `--run`.

## Before you start

1. Copy his file into the container (Excel's "CSV UTF-8" or plain CSV):

   ```sh
   sudo docker cp his-rules.csv org-dev-kafka-analyser-1:/tmp/his-rules.csv
   ```

2. `rule_outbox_evaluators` must list `broker_thresholds` and
   `cluster_metrics` (it does today). Otherwise `--run` refuses.
3. No `BATCHTEST - ...` rule may exist (finish or `--cleanup` the batch test).

## Dry run, then run

```sh
sudo docker exec -it org-dev-kafka-analyser-1 python scripts/apply_rule_plan.py \
    --csv /tmp/his-rules.csv --recipients Prod_Kafka_alerts@operative.com
# read the plan, then:
sudo docker exec -it org-dev-kafka-analyser-1 python scripts/apply_rule_plan.py \
    --csv /tmp/his-rules.csv --recipients Prod_Kafka_alerts@operative.com --run
```

| Option | Meaning |
|---|---|
| `--csv FILE` | His file. Required for a dry run and for `--run`. |
| `--recipients ADDR` | Required. One address at `@operative.com` or `@sintecmedia.com`; case is kept. |
| `--only a,b` | Plan only these alert types. The two lag types are planned **only** this way. |
| `--skip a,b` | Leave these alert types out. |
| `--run` | Make the changes. |
| `--revert BACKUP` | Undo from a backup (dry run unless `--run`). |
| `--list-lag-offenders` | Read-only list for the blacklist boxes (below). |

## How the file is read

- Rows are matched to live rules **by id**. The run refuses if `alert_type` or
  `cluster_id` differs, if an id is unknown, duplicated or missing from the
  file (use a fresh export).
- Compared: `info`, `warning`, `critical`, `min_rf`, `cooldown_minutes`,
  `reminder_hours`. They are compared as numbers, so 3, 3.0 and 3.00 are equal.
  Booleans may be TRUE/FALSE in any case.
- A **blank cell means no change**. A tier is never cleared.
- Ignored: `created_at`, `updated_at`, every tier cell of `close_wait_spike` and
  `broker_unreachable`, and the file's `enabled` and `email_enabled` (the plan
  sets them). If `name`, `severity`, `send_resolve_card` or `threshold` differ,
  the plan lists them as "not applied".

## The plan

- Every rule **except** rule 3 (CLOSE_WAIT; it stays off), the two lag types
  (unless named in `--only`) and any type in `--skip`.
- Each planned rule: `enabled=false` (Teams off) and `email_enabled=true`, plus
  his changes. Rule 5: `enabled=true` and `email_enabled=true`.
- Every row is checked first with the API's own validation: tier order,
  ranges, whole numbers, SLO 0 to 100, `min_rf` of at least 2, `cooldown_minutes`
  of 0 or more. **One invalid row refuses the whole run.** Note that equal
  tiers (for example warning 1 and critical 1) are invalid.
- "would fire now" reads the live value with the batch script's probes and
  compares it with the **new** tiers. SLO is breached strictly below a tier;
  message rate at or below; every other type at or above. It prints the tier
  that would be breached, `no`, or `n/a` for the two types without a probe. A
  fire still needs 2 consecutive checks (SLO: 2 hourly readings).

## What `--run` does

1. Refuses on any of the problems above.
2. Writes `/home/backups/manual/rules-before-apply-<UTC>.json` (mode 600,
   **inside the container**). It holds every `kafka_alert_configs` column and
   the two settings, including any rule webhook URL, so treat it as a secret.
   Webhooks are never printed.
3. Sets `email_enabled=true` and `email_recipients` through the live agent's
   `POST /settings` on localhost:8003, so the evaluator sees them.
4. Applies the rules one at a time through `routes_alerts.update_alert_config`.
5. Verifies the result and prints **VERIFICATION PASSED** or **FAILED**:
   - every planned rule has the intended toggles and tiers, and nothing else
     changed except `updated_at`;
   - every other rule is unchanged in every column;
   - the settings (read back with `GET /settings`) are as intended.

   On an error it stops, verifies and prints the `--revert` line. It does
   **not** roll back on its own.

Copy the backup out afterwards:
`sudo docker cp org-dev-kafka-analyser-1:/home/backups/manual/rules-before-apply-<UTC>.json /home/backups/manual/`

## Undo

```sh
sudo docker exec -it org-dev-kafka-analyser-1 python scripts/apply_rule_plan.py --revert /home/backups/manual/rules-before-apply-<UTC>.json [--run]
```

The undo restores each changed rule's `enabled`, `email_enabled`,
`cooldown_minutes`, `config` and `updated_at` exactly, then the two settings.
It then checks every rule in the backup against the database, byte for byte.
It refuses if a rule has disappeared or if a column it does not manage
(for example the name) changed since the backup. Rules created after the
backup are left alone.

## Lag blacklists first

```sh
sudo docker exec -it org-dev-kafka-analyser-1 python scripts/apply_rule_plan.py --list-lag-offenders
```

For each cluster's lag rule, this lists the consumer groups or connectors
whose lag (rows updated in the last 20 minutes) is at or above the rule's
**warning** tier, largest first. Names already blacklisted or with custom
tiers are marked. Each list ends with a names-only block to paste into the
rule's "Not alerted (blacklist)" box. Consumer lag also covers the
`connect-<name>` groups of connectors. Some group ids contain literal quote
characters, for example `'prod_mdm_grp'`; paste them as printed.

## Tests

`tests/test_apply_rule_plan.py` runs against a scratch Postgres that has the
live schema and a copy of the live rules. The docstring gives the
`docker run` line.
