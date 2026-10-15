# batch_test_rules.py: batch test of the alert rules

Tests each cluster rule type end to end on both channels (Teams card and
email), or on one with `--channels`: fire, optional escalation, resolve. It uses **temporary copies** named
`BATCHTEST - <type> - <cluster name>`, so the real rules are never edited.
Copies post to a **test** Teams webhook. Emails go to **one** address.

The default is a dry run: it only reads and prints the plan. Nothing changes
without `--run`.

## Before you start

1. Put the test webhook into the container. It never goes on a command line,
   into shell history or onto the host disk:

   ```sh
   read -rs HOOK      # paste the TEST channel's webhook URL, press Enter (not echoed)
   printf '%s' "$HOOK" | sudo docker exec -i org-dev-kafka-analyser-1 \
       sh -c 'umask 077; cat > /dev/shm/batch_webhook'
   unset HOOK
   ```

   `/dev/shm` is RAM only (tmpfs). The default path `/run/batch_webhook` would
   sit on the container's disk layer. Because of this, the commands below set
   `BATCH_TEST_WEBHOOK_FILE=/dev/shm/batch_webhook`. The script deletes the
   file at the end of every `--run`, so create it again before each run.

2. Do a dry run and read the plan:

   ```sh
   sudo docker exec -it -e BATCH_TEST_WEBHOOK_FILE=/dev/shm/batch_webhook org-dev-kafka-analyser-1 \
       python scripts/batch_test_rules.py --to you@sintecmedia.com
   ```

## Run

```sh
sudo docker exec -it -e BATCH_TEST_WEBHOOK_FILE=/dev/shm/batch_webhook org-dev-kafka-analyser-1 \
    python scripts/batch_test_rules.py --to you@sintecmedia.com --run [--log-file PATH]
```

`-it` is required, because the script asks for confirmations.

| Option | Meaning |
|---|---|
| `--to` | Required. Exactly one address at `@operative.com` or `@sintecmedia.com`. |
| `--only a,b` | Test only these types. The SLO and lag types can run only through this option. |
| `--cluster ID` | Test on this cluster only. By default each type uses the first cluster in the order 8, 10, 4, 9 where the type can be forced. |
| `--channels both\|email\|teams` | The copy's channels. `both` (the default) turns Teams and Email on. `email` turns Email on and Teams off: the run then checks that no card is posted (triggers saved with `Teams off for this rule`, the resolve's outbox Teams channel `skipped`) and that the fire and resolve emails were sent. `teams` turns Teams on and Email off. |
| `--escalate` | Fire at warning first, then escalate to critical. Used only where the value allows two tiers. |
| `--timeout-minutes N` | Per-rule wait limit. The default is 20 minutes (90 for SLO). |
| `--log-file PATH` | A copy of the agent log to search for the `rule email sent` lines. |
| `--cleanup` | Remove leftover BATCHTEST rules only. Add `--restore-settings` to also put back `email_enabled` and `email_recipients` from the newest backup. Add `--run` to make the change. |

## What a run does

### At the start

1. Writes a backup to `/home/backups/manual/rules-before-batch-<UTC>.json`
   (mode 600). The backup holds the whole `kafka_alert_configs` table plus the
   `email_enabled`, `email_recipients` and `teams_enabled` settings. It leaves
   out the webhooks of BATCHTEST copies. The path is **inside the container**:
   copy the file out with the `docker cp` line printed at the end.
2. Removes any leftover BATCHTEST rules.
3. Refuses to start, and says which check failed, if any of these is true:
   - another rule has email on or a webhook set;
   - `email_recipients` is not empty;
   - `teams_enabled` is off and `--channels` is `both` or `teams` (with
     `email` the run needs only `email_enabled`, which it turns on itself);
   - the test webhook is the agent-level webhook;
   - the evaluator schedule is disabled;
   - rule notices are pending in the outbox.
4. Sets `email_enabled=true` and `email_recipients=<--to>` through the live
   agent's `POST /settings`. The evaluator reads its own in-memory copy of the
   settings, so a direct database write would not reach it.

### For each type, one at a time

1. Reads the live value. It uses the evaluator's own SQL and probes, with the
   same guards.
2. Creates the copy with Teams and Email off, so it cannot run yet. The copy
   has the test webhook, the resolve card on and cooldown 0. Its tiers put the value clearly
   beyond critical. For below-direction types the value sits clearly under
   the critical tier.
3. Turns on the channels under test (`--channels`) in one change, then polls
   the trigger rows every 30 s.
4. When the copy fires, prints the trigger row, `teams_post_success`, the
   outbox record (email and Teams status) and the log line, or the host
   command that greps for it.
   When the type's evaluator sends through the notification outbox (its name
   is in the `rule_outbox_evaluators` setting), the worker sends after the
   trigger is written: the script first waits, up to 10 minutes, until the
   outbox rows of those triggers have settled both channels, prints the
   wait, and only then reads `teams_post_success` and `error_detail`. With
   the setting empty (today) nothing changes.
5. Asks **y** (seen), **n** (not seen) or **s** (skip the rest of this rule).
6. Relaxes the tiers, waits for the resolve, checks it the same way and asks
   again.
7. Deletes the copy.

The copy is created through `routes_alerts.create_alert_config`, so every API
validation runs. The one exception is the one-rule-per-type-per-cluster check,
which is skipped for that call only: every type already has a disabled default
rule on every cluster. Edits and deletes go through `update_alert_config` and
`delete_alert_config`.

### At the end

A `finally` block runs on success, on an error, on Ctrl+C, SIGTERM and SIGHUP.
Further signals are ignored while it runs. It:

1. Disables and deletes every BATCHTEST rule.
2. Restores both settings.
3. Prints the results table.
4. Prints a verification: same count of real rules, same enabled set (rule 5
   today), every real rule unchanged in every column, no BATCHTEST rule left,
   and all three settings back to their saved values.

The exit code is 0 when the verification passes, 1 when a check fails or an
error occurs, 2 on a refusal and 130 when interrupted.

## Timing (evaluator every 5 min, two breaching runs to fire)

| Type | Fire | Resolve |
|---|---|---|
| Most types | about 5-11 min | next evaluator run |
| `msg_rate_in` | 2nd new inflow run, about 5-15 min | next evaluator run |
| `slo_compliance` | first run after the next hourly row (written at :05, :20 for cluster 10), up to about 80 min | next evaluator run |

Allow about 15-20 minutes per type plus confirmation time.

## Types that cannot be forced

The API only accepts tiers greater than 0, and a rule fires when the value is
at or above a tier. So a healthy count of 0 can never fire. On 13 Oct this
applied to URP (0 on every cluster). It probably also applies to ZooKeeper
problems, Connect workers down and Schema Registry nodes down: these are
probed live, and the dry run shows their values. Such types are reported as
SKIP with the reason. A message rate of 0 (cluster 4) cannot resolve, so the
script picks another cluster.

## SLO and lag types

These run only with `--only`, and only after you type **YES**.

- **Lag rules** describe REAL lag. Clusters 8, 9 and 10 have groups far above
  any tier. To limit this, the copy fires for ONE name only: a custom row for
  the group or connector with the largest lag. The copy's generic tiers are
  set to 10^15, so no other name fires.
- **SLO** needs two distinct hourly readings.

## The "rule email sent" line

The container log cannot be read from inside the container. Without
`--log-file`, the in-container evidence is the outbox record (`email_status`
sent or failed, from the SMTP result). At each check the script prints the
host command that finds the line:

```sh
sudo docker logs --since <time> <container> 2>&1 | grep 'rule_id=<id> kind=fire '
```

## Recovery after a hard kill (kill -9, container restart)

```sh
sudo docker exec -it org-dev-kafka-analyser-1 python scripts/batch_test_rules.py --cleanup --restore-settings --run
```

The backup lives in the container's own disk layer: it survives a restart but not a
recreate. Copy it out after every run. Without it, put the two settings back with
`POST /settings` (`{"email_enabled": false, "email_recipients": ""}` today).

## Tests

`tests/test_batch_test_rules.py` runs against a scratch Postgres that has the
live schema. See its docstring for the `docker run` line.
