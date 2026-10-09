#!/usr/bin/env bash
# Installs everything needed to build and run Balancify v3 on Ubuntu 22.04/24.04:
# toolchain, Boost, the ZooKeeper C client, DPDK (built from source), libpqos
# (intel-cmt-cat), and optionally the ZooKeeper server and TRex.
#
#   sudo ./scripts/install_deps.sh            # build dependencies
#   sudo ./scripts/install_deps.sh --all      # plus ZooKeeper server and TRex
#
# Versions can be overridden through the environment.
set -euo pipefail

DPDK_VERSION="${DPDK_VERSION:-23.11.2}"
PQOS_VERSION="${PQOS_VERSION:-v24.05}"
ZOOKEEPER_VERSION="${ZOOKEEPER_VERSION:-3.8.4}"
TREX_VERSION="${TREX_VERSION:-v3.04}"
PREFIX="${PREFIX:-/usr/local}"
SRC_DIR="${SRC_DIR:-/opt/balancify-deps}"
JOBS="${JOBS:-$(nproc)}"

WITH_ZK_SERVER=0
WITH_TREX=0
for arg in "$@"; do
    case "$arg" in
        --all) WITH_ZK_SERVER=1; WITH_TREX=1 ;;
        --zookeeper-server) WITH_ZK_SERVER=1 ;;
        --trex) WITH_TREX=1 ;;
        *) echo "unknown option $arg" >&2; exit 1 ;;
    esac
done

if [[ $EUID -ne 0 ]]; then
    echo "run as root (sudo)" >&2
    exit 1
fi

mkdir -p "$SRC_DIR"

echo "==> system packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build meson pkg-config git curl wget ca-certificates xz-utils \
    python3 python3-pip python3-pyelftools \
    libnuma-dev libpcap-dev libelf-dev libbsd-dev libssl-dev \
    libboost-dev \
    libzookeeper-mt-dev \
    linux-tools-common linux-tools-generic \
    jq iproute2 pciutils kmod

echo "==> DPDK ${DPDK_VERSION}"
if ! pkg-config --exists libdpdk || [[ "$(pkg-config --modversion libdpdk)" != "${DPDK_VERSION}" ]]; then
    cd "$SRC_DIR"
    if [[ ! -d "dpdk-stable-${DPDK_VERSION}" ]]; then
        wget -q "https://fast.dpdk.org/rel/dpdk-${DPDK_VERSION}.tar.xz"
        tar xf "dpdk-${DPDK_VERSION}.tar.xz"
    fi
    cd "dpdk-stable-${DPDK_VERSION}"
    rm -rf build
    meson setup build --prefix="$PREFIX" -Dplatform=native -Denable_kmods=false -Dtests=false \
        -Denable_apps=dumpcap,pdump,proc-info,test-pmd
    ninja -C build -j "$JOBS"
    ninja -C build install
    ldconfig
fi

echo "==> intel-cmt-cat (libpqos) ${PQOS_VERSION}"
if [[ ! -f "$PREFIX/include/pqos.h" && ! -f /usr/include/pqos.h ]]; then
    cd "$SRC_DIR"
    rm -rf intel-cmt-cat
    git clone --depth 1 --branch "$PQOS_VERSION" https://github.com/intel/intel-cmt-cat.git
    make -C intel-cmt-cat/lib -j "$JOBS"
    make -C intel-cmt-cat/lib install PREFIX="$PREFIX"
    ldconfig
fi

if [[ $WITH_ZK_SERVER -eq 1 ]]; then
    echo "==> ZooKeeper server ${ZOOKEEPER_VERSION}"
    apt-get install -y --no-install-recommends openjdk-17-jre-headless
    cd /opt
    if [[ ! -d "apache-zookeeper-${ZOOKEEPER_VERSION}-bin" ]]; then
        wget -q "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
        tar xzf "apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
    fi
    ln -sfn "/opt/apache-zookeeper-${ZOOKEEPER_VERSION}-bin" /opt/zookeeper
fi

if [[ $WITH_TREX -eq 1 ]]; then
    echo "==> TRex ${TREX_VERSION}"
    cd /opt
    if [[ ! -d "$TREX_VERSION" ]]; then
        wget -q --no-check-certificate "https://trex-tgn.cisco.com/trex/release/${TREX_VERSION}.tar.gz"
        tar xzf "${TREX_VERSION}.tar.gz"
    fi
    ln -sfn "/opt/${TREX_VERSION}" /opt/trex
fi

echo "==> done"
echo "PKG_CONFIG_PATH should include: $PREFIX/lib/x86_64-linux-gnu/pkgconfig:$PREFIX/lib/pkgconfig"
