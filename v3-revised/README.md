# Balancify v3 (revised design)

DPDK implementation of the revised Balancify design: consistent hashing by
default, and per-connection state only for the connections whose
hash-selected DIP is more loaded than the pool average by more than a threshold
`T`. The table therefore grows with request heterogeneity, not with the number
of active connections.

## What changed relative to v2

| Area | v2 | v3 |
|---|---|---|
| Hash path | Maglev table rebuilt on every metrics update | Stable hashing: fixed bucket count, minimal reassignment on DIP churn, bucket table shared through ZooKeeper; Maglev selectable |
| Override rule | `cpu - avg > T` and an extra `alt < 0.7·cpu` condition | Exactly Algorithm 2: `load[h] - avg > T` → least-loaded target, recorded in the table |
| Override target | argmin | R0 argmin, R1 argmin + per-connection increment (default), R2 uniform among below-average DIPs |
| Load signal | CPU pushed over HTTP | CPU or request latency (agent reads the access log), UDP reports, mean over each τ |
| Monitoring | fixed 10 s, no health handling | τ and T adjustable at run time, DIP leaves the pool 1 s after its agent stops reporting, oscillation counter |
| Table | `std::unordered_map<std::string,…>` with a global lock | per-core open-addressed table, 64-byte buckets with 16-bit fingerprints, incremental resize, lazy reclamation on FIN/RST/inactivity |
| Bloom filter | always on, mutex, SHA-256 | cache-blocked counting filter, 4-bit counters with exact overflow table, enabled only when the table outgrows the cache budget |
| Multi-instance | none | overrides written to ZooKeeper before the first packet is forwarded, local copies on every instance, ownership follows ECMP, leader GC, sync before ECMP announce |
| DIP churn | none | removal purges the DIP's entries and buckets; addition never touches the table |
| Data plane | single queue, `route()` per packet under a lock, header rewrite | RSS over N cores, no shared locks, IP-in-IP encapsulation with DSR, ARP for the LB address |
| Baselines | separate programs | `mode = balancify \| all_stateful \| stateless` in the same binary |
| Intel CAT | class id = lcore | partition sized in MB, associated with all forwarding cores, optional isolation of other workloads |

## Layout

```
v3-revised/
├── load_balancer/        DPDK load balancer (balancify_lb)
│   ├── include/ src/
│   │   ├── worker        per-core fast path (Algorithm 2)
│   │   ├── conn_table    connection-to-DIP table
│   │   ├── counting_bloom
│   │   ├── hash_path     stable hashing / Maglev, RCU-published bucket table
│   │   ├── load_monitor  agent reports, health, τ sampling (Algorithm 1)
│   │   ├── override_policy  threshold gate and R0/R1/R2
│   │   ├── coordinator   ZooKeeper: bindings, bucket table, election, GC
│   │   ├── dataplane     port, RSS, rings, software RSS for binding placement
│   │   ├── cache_allocation  Intel CAT through libpqos
│   │   └── control_api, stats
├── agent/                per-DIP agent (balancify_agent)
├── common/include/       agent report wire format
├── testbed/              backend service and closed-loop workload generator
├── trex/                 TRex profiles W1 (64 B), W2 (IMIX), W3 (trace replay)
├── scripts/              dependencies, build, host/DIP setup, runs, sweeps, perf counters
├── deploy/zookeeper/     three-node ensemble
└── config/               example configuration
```

## Build

```bash
sudo ./scripts/install_deps.sh          # toolchain, Boost, ZooKeeper C client, DPDK, libpqos
./scripts/build.sh
```

Optional parts can be switched off: `-DBALANCIFY_WITH_ZOOKEEPER=OFF`
(single instance), `-DBALANCIFY_WITH_PQOS=OFF` (no CAT),
`-DBALANCIFY_BUILD_TESTBED=OFF`. `sudo ./scripts/install_deps.sh --all`
also installs a ZooKeeper server and TRex under `/opt`.

## Run

Load-balancer host:

```bash
sudo ./scripts/setup_host.sh 0000:3b:00.0 4096
cp config/balancify.conf.example /etc/balancify.conf     # edit VIP, addresses, DIPs, zk_hosts
sudo ./scripts/run_lb.sh 0000:3b:00.0 0-2 /etc/balancify.conf
```

Lcore 0 runs the control threads; lcores 1–2 forward packets, one RSS queue
each. Every parameter can be overridden: `--set mode=all_stateful`,
`--set threshold=15`, `--set bloom=off`, `--set cat_partition_mb=2`.

Each DIP (IP-in-IP decapsulation, VIP on loopback, agent):

```bash
sudo ACCESS_LOG=/var/log/nginx/access.log ./scripts/setup_dip.sh \
     10.0.0.100 10.0.3.1 10.0.1.11:7001,10.0.1.12:7001,10.0.1.13:7001 80
```

Several instances behind ECMP: start a ZooKeeper ensemble
(`deploy/zookeeper/docker-compose.yml`), give every instance the same
`service`, a distinct `instance_id`, and the same `zk_hosts`. An instance
announces the VIP through `announce_cmd` only after it has loaded every
shared binding. The bucket table is about 130 KB at 65 536 buckets; larger
bucket counts need a larger `jute.maxbuffer`, as set in the compose file.

## Control API

```bash
curl -s localhost:8080/stats | jq
curl -s localhost:8080/dips  | jq
curl -s -X POST 'localhost:8080/params?threshold=15&interval=5&rule=r2'
curl -s -X POST 'localhost:8080/dips/drain?name=video-07'
curl -s -X POST 'localhost:8080/dips/enable?name=video-07'
curl -s -X POST 'localhost:8080/dips/add?name=video-17&ip=10.0.3.17'
```

With `stats_log` set, one CSV line per second records packet and connection
rates, override fraction, table entries and bytes, Bloom-filter state and
measured false-positive rate, cycles per packet, coordination write rates,
and the load spread across DIPs.

## Experiments (Section V)

| Experiment | How |
|---|---|
| Load spread and table size (V-A, V-B) | `stats_log` on each instance: `load_stddev`, `table_entries`, `table_bytes` |
| Throughput and per-packet cost (V-C) | TRex `trex/w1_64b.py`, `w2_imix.py`, `w3_replay.py`; `scripts/perf_counters.sh` for cycles and LLC misses per packet; `mode=all_stateful` for the same-binary stateful configuration, `mode=stateless` for hashing only |
| End-to-end latency (V-D) | `testbed/` backend on the DIPs and `balancify_workload --cv <c>` on the clients |
| T, τ, target rule (V-E) | `scripts/sweep.sh threshold\|interval\|rule` |
| Cache budget and Bloom filter (V-F) | `--set cat_partition_mb=8\|4\|2\|1\|0`, `--set bloom=on\|off`, `cat_isolate_others=true` with an antagonist |
| DIP and LB churn (V-G) | `/dips/add`, `/dips/drain`, stopping an agent, stopping an instance; coordinator counters in `/stats` |
| Scalability (V-H) | EAL `-l` with 1–12 forwarding lcores; DIP pools from 8 to 256 |
| Load signal (V-I) | `load_signal = latency` with the agent reading the access log; `threshold` in ms |
