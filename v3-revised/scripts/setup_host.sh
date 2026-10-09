#!/usr/bin/env bash
# Prepares a load-balancer host: hugepages, vfio-pci binding of the data-plane
# NIC, the MSR/resctrl interfaces used by Intel CAT, and the CPU settings of
# the testbed (no Turbo, fixed frequency).
#
#   sudo ./scripts/setup_host.sh <pci-address> [hugepages-2M]
#   sudo ./scripts/setup_host.sh 0000:3b:00.0 4096
set -euo pipefail

PCI="${1:?usage: $0 <pci-address> [hugepages-2M]}"
NR_HUGEPAGES="${2:-4096}"
PREFIX="${PREFIX:-/usr/local}"

if [[ $EUID -ne 0 ]]; then
    echo "run as root (sudo)" >&2
    exit 1
fi

echo "==> hugepages (${NR_HUGEPAGES} x 2 MB)"
for node in /sys/devices/system/node/node*; do
    echo "$NR_HUGEPAGES" > "$node/hugepages/hugepages-2048kB/nr_hugepages"
done
mkdir -p /dev/hugepages
mountpoint -q /dev/hugepages || mount -t hugetlbfs nodev /dev/hugepages

echo "==> vfio-pci for ${PCI}"
modprobe vfio-pci
DEVBIND="$(command -v dpdk-devbind.py || echo "$PREFIX/bin/dpdk-devbind.py")"
IFACE="$(ls "/sys/bus/pci/devices/${PCI}/net" 2>/dev/null || true)"
if [[ -n "$IFACE" ]]; then
    ip link set "$IFACE" down || true
fi
"$DEVBIND" --bind=vfio-pci "$PCI"
"$DEVBIND" --status-dev net | grep -E "$PCI" || true

echo "==> Intel CAT interfaces"
modprobe msr || true
if ! mountpoint -q /sys/fs/resctrl; then
    mount -t resctrl resctrl /sys/fs/resctrl 2>/dev/null || echo "resctrl not available; use cat_interface = msr"
fi

echo "==> CPU frequency"
if [[ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
    echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
fi
if command -v cpupower >/dev/null 2>&1; then
    cpupower frequency-set -g performance >/dev/null || true
fi

echo "==> done"
