#!/usr/bin/env bash
# Starts one load-balancer instance.
#
#   sudo ./scripts/run_lb.sh <pci-address> <lcores> <config> [--set key=value ...]
#   sudo ./scripts/run_lb.sh 0000:3b:00.0 0-2 config/balancify.conf
#   sudo ./scripts/run_lb.sh 0000:3b:00.0 0-2 config/balancify.conf --set mode=all_stateful
#
# The first lcore runs the control threads; every other lcore forwards packets
# from its own RSS queue.
set -euo pipefail

PCI="${1:?usage: $0 <pci-address> <lcores> <config> [--set key=value ...]}"
LCORES="${2:?missing lcores}"
CONFIG="${3:?missing config}"
shift 3

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LB="${LB:-$ROOT/build/load_balancer/balancify_lb}"
MEM_CHANNELS="${MEM_CHANNELS:-4}"
FILE_PREFIX="${FILE_PREFIX:-balancify}"

exec "$LB" -l "$LCORES" -n "$MEM_CHANNELS" -a "$PCI" --file-prefix "$FILE_PREFIX" -- --config "$CONFIG" "$@"
