#!/usr/bin/env bash
# Configures and builds Balancify v3.
#
#   ./scripts/build.sh                         # Release build in ./build
#   ./scripts/build.sh -DBALANCIFY_WITH_PQOS=OFF
#   BUILD_DIR=build-debug BUILD_TYPE=Debug ./scripts/build.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT/build}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
PREFIX="${PREFIX:-/usr/local}"

export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}:$PREFIX/lib/x86_64-linux-gnu/pkgconfig:$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig"

GENERATOR=()
if command -v ninja >/dev/null 2>&1; then
    GENERATOR=(-G Ninja)
fi

cmake -S "$ROOT" -B "$BUILD_DIR" "${GENERATOR[@]}" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_PREFIX_PATH="$PREFIX" \
    "$@"
cmake --build "$BUILD_DIR" -j "$(nproc)"

echo
echo "binaries:"
ls -1 "$BUILD_DIR"/load_balancer/balancify_lb "$BUILD_DIR"/agent/balancify_agent \
      "$BUILD_DIR"/testbed/balancify_backend "$BUILD_DIR"/testbed/balancify_workload 2>/dev/null || true
