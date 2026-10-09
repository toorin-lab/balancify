#!/usr/bin/env bash
# Parameter sweeps of Section V-E through the control API. For every value
# the parameter is set, the system settles for one warm-up period, and the
# load spread, override fraction, table size and oscillation count are
# averaged over the measurement period.
#
#   ./scripts/sweep.sh threshold "5 10 15 20 25 30 40" [warmup-s] [measure-s] [url] > sweep_T.csv
#   ./scripts/sweep.sh interval "1 2 5 10 20 30"        [warmup-s] [measure-s] [url] > sweep_tau.csv
#   ./scripts/sweep.sh rule "r0 r1 r2"                  [warmup-s] [measure-s] [url] > sweep_rule.csv
set -euo pipefail

PARAM="${1:?usage: $0 threshold|interval|rule \"values\" [warmup-s] [measure-s] [url]}"
VALUES="${2:?missing values}"
WARMUP="${3:-60}"
MEASURE="${4:-300}"
URL="${5:-http://127.0.0.1:8080}"
SAMPLE_EVERY=10

stats() { curl -s "$URL/stats"; }

echo "param,value,load_stddev_mean,override_fraction,table_entries_mean,oscillations,new_conns,overrides"
for v in $VALUES; do
    curl -s -X POST "$URL/params?${PARAM}=${v}" >/dev/null
    sleep "$WARMUP"
    s0="$(stats)"
    sum_sd=0
    sum_entries=0
    n=0
    end=$((SECONDS + MEASURE))
    while [[ $SECONDS -lt $end ]]; do
        s="$(stats)"
        sum_sd="$(awk -v a="$sum_sd" -v b="$(echo "$s" | jq '.monitor.load_stddev')" 'BEGIN{print a+b}')"
        sum_entries="$(awk -v a="$sum_entries" -v b="$(echo "$s" | jq '.dataplane.table_entries')" 'BEGIN{print a+b}')"
        n=$((n + 1))
        sleep "$SAMPLE_EVERY"
    done
    s1="$(stats)"
    nc=$(( $(echo "$s1" | jq '.dataplane.new_conns') - $(echo "$s0" | jq '.dataplane.new_conns') ))
    ov=$(( $(echo "$s1" | jq '.dataplane.overrides') - $(echo "$s0" | jq '.dataplane.overrides') ))
    osc=$(( $(echo "$s1" | jq '.monitor.oscillations') - $(echo "$s0" | jq '.monitor.oscillations') ))
    awk -v p="$PARAM" -v v="$v" -v sd="$sum_sd" -v e="$sum_entries" -v n="$n" -v nc="$nc" -v ov="$ov" -v osc="$osc" \
        'BEGIN{printf "%s,%s,%.3f,%.4f,%.0f,%d,%d,%d\n", p, v, sd/n, (nc>0?ov/nc:0), e/n, osc, nc, ov}'
done
