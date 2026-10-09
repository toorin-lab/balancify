#!/usr/bin/env bash
# Configures a backend server (DIP) for IP-in-IP with direct server return:
# the VIP lives on the loopback, decapsulated packets are accepted, and the
# server never answers ARP for the VIP. Then starts the monitoring agent.
#
#   sudo ./scripts/setup_dip.sh <vip> <dip-ip> <lb1:port>[,<lb2:port>...] [service-port]
#   sudo ./scripts/setup_dip.sh 10.0.0.100 10.0.3.1 10.0.1.11:7001,10.0.1.12:7001,10.0.1.13:7001 80
set -euo pipefail

VIP="${1:?usage: $0 <vip> <dip-ip> <lb-list> [service-port]}"
DIP="${2:?missing dip-ip}"
LBS="${3:?missing lb list}"
SERVICE_PORT="${4:-80}"
AGENT="${AGENT:-$(dirname "$0")/../build/agent/balancify_agent}"
ACCESS_LOG="${ACCESS_LOG:-}"
LATENCY_FIELD="${LATENCY_FIELD:--1}"
LATENCY_UNIT="${LATENCY_UNIT:-s}"

if [[ $EUID -ne 0 ]]; then
    echo "run as root (sudo)" >&2
    exit 1
fi

modprobe ipip
ip link set tunl0 up
ip addr replace "${VIP}/32" dev lo

sysctl -qw net.ipv4.conf.all.rp_filter=0
sysctl -qw net.ipv4.conf.default.rp_filter=0
sysctl -qw net.ipv4.conf.tunl0.rp_filter=0
sysctl -qw net.ipv4.conf.all.arp_ignore=1
sysctl -qw net.ipv4.conf.all.arp_announce=2

ARGS=(--dip-ip "$DIP" --service-port "$SERVICE_PORT" --check-port "$SERVICE_PORT" --interval-ms 200 --window-ms 1000)
IFS=',' read -ra LB_LIST <<< "$LBS"
for lb in "${LB_LIST[@]}"; do
    ARGS+=(--lb "$lb")
done
if [[ -n "$ACCESS_LOG" ]]; then
    ARGS+=(--access-log "$ACCESS_LOG" --latency-field "$LATENCY_FIELD" --latency-unit "$LATENCY_UNIT")
fi

echo "starting agent: $AGENT ${ARGS[*]}"
exec "$AGENT" "${ARGS[@]}"
