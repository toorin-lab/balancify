#!/usr/bin/env bash
# Cycles and LLC load misses per forwarded packet on the forwarding cores
# (Table I). Reads the packet count from the control API before and after a
# perf window over the given CPUs.
#
#   sudo ./scripts/perf_counters.sh <cpu-list> [seconds] [control-url]
#   sudo ./scripts/perf_counters.sh 1-2 30 http://127.0.0.1:8080
set -euo pipefail

CPUS="${1:?usage: $0 <cpu-list> [seconds] [control-url]}"
SECONDS_WINDOW="${2:-30}"
URL="${3:-http://127.0.0.1:8080}"

rx() { curl -s "$URL/stats" | jq '.dataplane.rx_pkts'; }

BEFORE="$(rx)"
OUT="$(perf stat -x, -e cycles,instructions,LLC-load-misses,LLC-loads -C "$CPUS" -- sleep "$SECONDS_WINDOW" 2>&1 >/dev/null)"
AFTER="$(rx)"
PKTS=$((AFTER - BEFORE))

cycles="$(echo "$OUT" | awk -F, '$3=="cycles"{print $1}')"
instr="$(echo "$OUT" | awk -F, '$3=="instructions"{print $1}')"
misses="$(echo "$OUT" | awk -F, '$3=="LLC-load-misses"{print $1}')"
loads="$(echo "$OUT" | awk -F, '$3=="LLC-loads"{print $1}')"

echo "packets           $PKTS"
echo "mpps              $(awk -v p="$PKTS" -v s="$SECONDS_WINDOW" 'BEGIN{printf "%.2f", p/s/1e6}')"
if [[ "$PKTS" -gt 0 ]]; then
    echo "cycles/pkt        $(awk -v c="$cycles" -v p="$PKTS" 'BEGIN{printf "%.1f", c/p}')"
    echo "instructions/pkt  $(awk -v c="$instr" -v p="$PKTS" 'BEGIN{printf "%.1f", c/p}')"
    echo "LLC misses/pkt    $(awk -v c="$misses" -v p="$PKTS" 'BEGIN{printf "%.3f", c/p}')"
    echo "LLC loads/pkt     $(awk -v c="$loads" -v p="$PKTS" 'BEGIN{printf "%.3f", c/p}')"
fi
