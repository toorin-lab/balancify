#!/usr/bin/env bash
# announce_cmd / withdraw_cmd hook: tells the local BGP speaker to start or
# stop advertising the VIP, which adds or removes this instance from the
# routers' ECMP group. Balancify runs "up" only after its local copy of the
# shared bindings is synchronized, and "down" before it stops forwarding.
#
#   announce_cmd = /opt/balancify/scripts/announce_vip.sh up
#   withdraw_cmd = /opt/balancify/scripts/announce_vip.sh down
#
# BIRD: the VIP is exported by a protocol named $BIRD_PROTOCOL (default vip).
# ExaBGP: set EXABGP_PIPE and VIP to write to the ExaBGP command pipe instead.
set -euo pipefail

ACTION="${1:?usage: $0 up|down}"
BIRD_PROTOCOL="${BIRD_PROTOCOL:-vip}"

if [[ -n "${EXABGP_PIPE:-}" ]]; then
    : "${VIP:?VIP must be set with EXABGP_PIPE}"
    NEXT_HOP="${NEXT_HOP:-self}"
    case "$ACTION" in
        up) echo "announce route ${VIP}/32 next-hop ${NEXT_HOP}" > "$EXABGP_PIPE" ;;
        down) echo "withdraw route ${VIP}/32 next-hop ${NEXT_HOP}" > "$EXABGP_PIPE" ;;
        *) echo "unknown action $ACTION" >&2; exit 1 ;;
    esac
    exit 0
fi

case "$ACTION" in
    up) birdc enable "$BIRD_PROTOCOL" ;;
    down) birdc disable "$BIRD_PROTOCOL" ;;
    *) echo "unknown action $ACTION" >&2; exit 1 ;;
esac
