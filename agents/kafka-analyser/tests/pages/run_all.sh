#!/usr/bin/env bash
# Runs every page stub in this folder and prints its PASS/FAIL counts
# (lines starting with [PASS]/[FAIL] or PASS/FAIL).
# Needs node and jsdom on NODE_PATH (see README.md). Exits 1 if any check fails.
cd "$(dirname "$0")" || exit 2
total_fail=0
for f in [a-z]*.js; do
  out=$(timeout 300 node "$f" 2>&1); code=$?
  pass=$(grep -cE '^ *(\[PASS\]|PASS )' <<<"$out"); fail=$(grep -cE '^ *(\[FAIL\]|FAIL )' <<<"$out")
  [ "$code" -ne 0 ] && [ "$fail" -eq 0 ] && fail=1   # crashed or exited non-zero
  printf '%-30s pass %3d  fail %d\n' "$f" "$pass" "$fail"
  [ "$fail" -ne 0 ] && { grep -E '^ *(\[FAIL\]|FAIL )' <<<"$out" | cut -c1-200 | sed 's/^/    /'; total_fail=$((total_fail + fail)); }
done
echo "failed checks: $total_fail"
[ "$total_fail" -eq 0 ]
