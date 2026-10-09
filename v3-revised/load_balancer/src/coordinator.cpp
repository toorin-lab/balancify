#include "coordinator.hpp"

#include <iostream>

#ifdef BALANCIFY_HAS_ZOOKEEPER

#include <zookeeper/zookeeper.h>

#include <rte_mbuf.h>
#include <rte_ring.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace balancify::lb {

namespace {

constexpr int kShards = 256;
constexpr uint64_t kGcPeriodMs = 1000;

uint64_t steady_ms() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

std::string hex_ip(uint32_t ip_be) {
    const auto* b = reinterpret_cast<const uint8_t*>(&ip_be);
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%02x%02x%02x%02x", b[0], b[1], b[2], b[3]);
    return buf;
}

bool parse_hex_ip(std::string_view s, uint32_t& ip_be) {
    if (s.size() != 8) return false;
    auto* b = reinterpret_cast<uint8_t*>(&ip_be);
    for (int i = 0; i < 4; ++i) {
        unsigned v = 0;
        if (std::sscanf(std::string(s.substr(2 * i, 2)).c_str(), "%2x", &v) != 1) return false;
        b[i] = static_cast<uint8_t>(v);
    }
    return true;
}

int shard_of(const FiveTuple& t) {
    return static_cast<int>(hash_tuple(t, kSeedShard) & (kShards - 1));
}

std::string shard_name(int shard) {
    char buf[3];
    std::snprintf(buf, sizeof(buf), "%02x", shard);
    return buf;
}

struct ParsedBinding {
    std::string key;
    FiveTuple tuple;
    uint32_t dip_ip{0};
    std::string owner;
};

// <26 hex tuple>.<8 hex dip>.<owner>
bool parse_binding(std::string_view name, ParsedBinding& out) {
    const auto p1 = name.find('.');
    if (p1 == std::string_view::npos) return false;
    const auto p2 = name.find('.', p1 + 1);
    if (p2 == std::string_view::npos) return false;
    out.key = std::string(name.substr(0, p1));
    out.owner = std::string(name.substr(p2 + 1));
    return !out.owner.empty() && tuple_from_hex(out.key, out.tuple) &&
           parse_hex_ip(name.substr(p1 + 1, p2 - p1 - 1), out.dip_ip);
}

// Bucket table encoding: "BKT1", version (u64), nbuckets (u32), ndips (u16),
// ndips DIP addresses (network order), nbuckets u16 positions into that list.
std::string encode_buckets(uint64_t version, const std::vector<uint16_t>& buckets, const DipPool& pool) {
    std::vector<uint32_t> ips;
    std::unordered_map<uint16_t, uint16_t> pos;
    for (uint16_t d : buckets) {
        if (d == kInvalidDip || pos.count(d)) continue;
        pos[d] = static_cast<uint16_t>(ips.size());
        ips.push_back(pool[d].ip_be);
    }
    const auto nb = static_cast<uint32_t>(buckets.size());
    const auto nd = static_cast<uint16_t>(ips.size());
    std::string out;
    out.resize(4 + 8 + 4 + 2 + size_t(nd) * 4 + size_t(nb) * 2);
    char* p = out.data();
    std::memcpy(p, "BKT1", 4); p += 4;
    std::memcpy(p, &version, 8); p += 8;
    std::memcpy(p, &nb, 4); p += 4;
    std::memcpy(p, &nd, 2); p += 2;
    for (uint32_t ip : ips) { std::memcpy(p, &ip, 4); p += 4; }
    for (uint16_t d : buckets) {
        const uint16_t v = d == kInvalidDip ? kInvalidDip : pos[d];
        std::memcpy(p, &v, 2);
        p += 2;
    }
    return out;
}

bool decode_buckets(const char* data, size_t len, DipPool& pool, uint64_t& version, std::vector<uint16_t>& out) {
    if (len < 18 || std::memcmp(data, "BKT1", 4) != 0) return false;
    const char* p = data + 4;
    uint32_t nb = 0;
    uint16_t nd = 0;
    std::memcpy(&version, p, 8); p += 8;
    std::memcpy(&nb, p, 4); p += 4;
    std::memcpy(&nd, p, 2); p += 2;
    if (len != 18 + size_t(nd) * 4 + size_t(nb) * 2) return false;
    std::vector<uint16_t> local(nd);
    for (uint16_t i = 0; i < nd; ++i) {
        uint32_t ip = 0;
        std::memcpy(&ip, p, 4);
        p += 4;
        uint16_t idx = pool.find_ip(ip);
        if (idx == kInvalidDip) idx = pool.add("", ip, nullptr, 1);
        local[i] = idx;
    }
    out.assign(nb, kInvalidDip);
    for (uint32_t b = 0; b < nb; ++b) {
        uint16_t v = 0;
        std::memcpy(&v, p, 2);
        p += 2;
        out[b] = v < nd ? local[v] : kInvalidDip;
    }
    return true;
}

void session_watcher(zhandle_t*, int type, int state, const char*, void* ctx);
void shard_watcher(zhandle_t*, int type, int state, const char*, void* ctx);
void shard_children(int rc, const String_vector* names, const void* data);
void instances_watcher(zhandle_t*, int type, int state, const char*, void* ctx);
void instances_children(int rc, const String_vector* names, const void* data);
void election_watcher(zhandle_t*, int type, int state, const char*, void* ctx);
void election_children(int rc, const String_vector* names, const void* data);
void buckets_watcher(zhandle_t*, int type, int state, const char*, void* ctx);
void buckets_data(int rc, const char* value, int len, const Stat*, const void* data);
void buckets_exists(int rc, const Stat*, const void* data);
void create_done(int rc, const char*, const void* data);
void delete_done(int rc, const void* data);
void adopt_done(int rc, const void* data);

} // namespace

struct Coordinator::Impl {
    struct Binding {
        std::string name;
        FiveTuple tuple;
        uint32_t dip_ip{0};
        std::string owner;
        uint64_t orphan_since_ms{0};
    };
    struct Held {
        unsigned worker{0};
        rte_mbuf* m{nullptr};
        uint64_t deadline_ms{0};
    };
    struct ShardCtx {
        Impl* self{nullptr};
        int shard{0};
    };
    struct OpCtx {
        Impl* self{nullptr};
        uint64_t held_id{0};
    };
    struct AdoptCtx {
        Impl* self{nullptr};
        std::string old_path;
        std::string new_path;
        zoo_op_t ops[2];
        zoo_op_result_t results[2];
        char path_buf[512];
    };

    Impl(Runtime& r, HashPathManager& h, std::function<std::vector<uint16_t>()> e)
        : rt(r), hpm(h), eligible_fn(std::move(e)) {
        root = rt.cfg.zk_root;
        while (!root.empty() && root.back() == '/') root.pop_back();
        root += "/" + rt.cfg.service;
        instances_path = root + "/instances";
        election_path = root + "/election";
        buckets_path = root + "/buckets";
        bindings_path = root + "/bindings";
        for (int s = 0; s < kShards; ++s) shard_ctx[s] = ShardCtx{this, s};
        orphan_timeout_ms = uint64_t(rt.cfg.zk_orphan_timeout_s != 0 ? rt.cfg.zk_orphan_timeout_s : rt.cfg.idle_timeout_s) * 1000;
    }

    Runtime& rt;
    HashPathManager& hpm;
    std::function<std::vector<uint16_t>()> eligible_fn;

    std::string root, instances_path, election_path, buckets_path, bindings_path;
    std::atomic<zhandle_t*> zh{nullptr};

    mutable std::mutex mu;
    std::condition_variable cv;
    bool connected{false};
    bool expired{false};
    std::string owner_id;
    std::string election_node;
    std::array<std::unordered_map<std::string, Binding>, kShards> shards;
    std::unordered_set<std::string> live;
    std::array<bool, kShards> shard_seen{};
    int shards_seen{0};
    bool instances_seen{false};
    bool election_seen{false};
    bool buckets_seen{false};
    bool buckets_exist{false};

    std::atomic<bool> is_leader{false};
    std::atomic<bool> leadership_changed{false};
    std::atomic<bool> synced{false};
    std::atomic<bool> running{false};
    std::thread thread;
    std::array<ShardCtx, kShards> shard_ctx;
    uint64_t orphan_timeout_ms{0};
    std::atomic<uint64_t> last_gc_ms{0};
    double sync_ms{0.0};

    std::mutex held_mu;
    std::map<uint64_t, Held> held;
    uint64_t next_held_id{0};

    std::atomic<uint64_t> creates{0}, create_errors{0}, deletes{0}, adoptions{0}, held_count{0}, released{0},
        timeouts{0}, fail_open{0}, gc_deletes{0}, session_restarts{0}, bucket_publishes{0};

    std::string binding_path(const FiveTuple& t, uint32_t dip_ip, const std::string& owner) const {
        return bindings_path + "/" + shard_name(shard_of(t)) + "/" + tuple_to_hex(t) + "." + hex_ip(dip_ip) + "." + owner;
    }

    std::string current_owner() const {
        std::lock_guard lk(mu);
        return owner_id;
    }

    bool is_connected() const {
        std::lock_guard lk(mu);
        return connected;
    }

    // --- session --------------------------------------------------------------

    bool connect(uint32_t timeout_ms) {
        zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
        zhandle_t* h = zookeeper_init(rt.cfg.zk_hosts.c_str(), session_watcher,
                                      static_cast<int>(rt.cfg.zk_session_timeout_ms), nullptr, this, 0);
        if (h == nullptr) {
            std::cerr << "[coordinator] zookeeper_init failed" << std::endl;
            return false;
        }
        zh.store(h);
        std::unique_lock lk(mu);
        if (!cv.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] { return connected; })) {
            std::cerr << "[coordinator] cannot reach " << rt.cfg.zk_hosts << std::endl;
            return false;
        }
        char sid[32];
        std::snprintf(sid, sizeof(sid), "%llx", static_cast<unsigned long long>(zoo_client_id(h)->client_id));
        owner_id = rt.cfg.instance_id + "-" + sid;
        return true;
    }

    void on_session_event(int state) {
        std::lock_guard lk(mu);
        if (state == ZOO_CONNECTED_STATE) {
            connected = true;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            connected = false;
            expired = true;
        } else if (state == ZOO_CONNECTING_STATE || state == ZOO_ASSOCIATING_STATE) {
            connected = false;
        }
        cv.notify_all();
    }

    void ensure(const std::string& path) {
        zoo_create(zh.load(), path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
    }

    bool register_instance() {
        zhandle_t* h = zh.load();
        std::string prefix;
        size_t pos = 1;
        while (pos <= root.size()) {
            const size_t next = root.find('/', pos);
            prefix = root.substr(0, next == std::string::npos ? root.size() : next);
            ensure(prefix);
            if (next == std::string::npos) break;
            pos = next + 1;
        }
        ensure(instances_path);
        ensure(election_path);
        ensure(bindings_path);
        String_vector existing{};
        std::unordered_set<std::string> have;
        if (zoo_get_children(h, bindings_path.c_str(), 0, &existing) == ZOK) {
            for (int i = 0; i < existing.count; ++i) have.insert(existing.data[i]);
            deallocate_String_vector(&existing);
        }
        for (int s = 0; s < kShards; ++s) {
            if (!have.count(shard_name(s))) ensure(bindings_path + "/" + shard_name(s));
        }

        const std::string owner = current_owner();
        const std::string me = instances_path + "/" + owner;
        int rc = zoo_create(h, me.c_str(), rt.cfg.instance_id.c_str(), static_cast<int>(rt.cfg.instance_id.size()),
                            &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, nullptr, 0);
        if (rc != ZOK) {
            std::cerr << "[coordinator] cannot register " << me << ": " << zerror(rc) << std::endl;
            return false;
        }
        char created[512];
        const std::string candidate = election_path + "/n_";
        rc = zoo_create(h, candidate.c_str(), owner.c_str(), static_cast<int>(owner.size()), &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_EPHEMERAL | ZOO_SEQUENCE, created, sizeof(created));
        if (rc != ZOK) {
            std::cerr << "[coordinator] cannot join the election: " << zerror(rc) << std::endl;
            return false;
        }
        std::lock_guard lk(mu);
        const std::string full(created);
        election_node = full.substr(full.rfind('/') + 1);
        return true;
    }

    void watch_all() {
        watch_instances();
        watch_election();
        if (rt.cfg.hashing == HashingScheme::Stable) {
            watch_buckets();
        } else {
            std::lock_guard lk(mu);
            buckets_seen = true;
        }
        for (int s = 0; s < kShards; ++s) watch_shard(s);
    }

    bool wait_synced(uint32_t timeout_ms) {
        std::unique_lock lk(mu);
        return cv.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
            return instances_seen && election_seen && buckets_seen && shards_seen == kShards;
        });
    }

    void reconnect() {
        CoreMsg m{};
        m.type = CoreMsgType::DisownAll;
        rt.broadcast(m);
        if (zhandle_t* old = zh.exchange(nullptr)) zookeeper_close(old);
        {
            std::lock_guard lk(mu);
            connected = false;
            expired = false;
            election_node.clear();
        }
        is_leader.store(false);
        session_restarts.fetch_add(1);
        if (!connect(rt.cfg.zk_sync_timeout_ms) || !register_instance()) {
            std::lock_guard lk(mu);
            expired = true;  // retried by the coordination thread
            return;
        }
        watch_all();
    }

    // --- watches ----------------------------------------------------------------

    void watch_shard(int s) {
        const std::string path = bindings_path + "/" + shard_name(s);
        zoo_awget_children(zh.load(), path.c_str(), shard_watcher, &shard_ctx[s], shard_children, &shard_ctx[s]);
    }

    void watch_instances() {
        zoo_awget_children(zh.load(), instances_path.c_str(), instances_watcher, this, instances_children, this);
    }

    void watch_election() {
        zoo_awget_children(zh.load(), election_path.c_str(), election_watcher, this, election_children, this);
    }

    void watch_buckets() {
        zoo_awget(zh.load(), buckets_path.c_str(), buckets_watcher, this, buckets_data, this);
    }

    void apply_shard(int s, const String_vector* names) {
        std::unordered_map<std::string, Binding> fresh;
        for (int i = 0; i < names->count; ++i) {
            ParsedBinding pb;
            if (!parse_binding(names->data[i], pb)) continue;
            fresh[pb.key] = Binding{names->data[i], pb.tuple, pb.dip_ip, pb.owner, 0};
        }
        std::vector<CoreMsg> out;
        {
            std::lock_guard lk(mu);
            const uint64_t now = steady_ms();
            auto& cur = shards[s];
            for (const auto& [key, b] : cur) {
                if (!fresh.count(key) && b.owner != owner_id) {
                    CoreMsg m{};
                    m.type = CoreMsgType::UnbindRemote;
                    m.tuple = b.tuple;
                    out.push_back(m);
                }
            }
            for (auto& [key, nb] : fresh) {
                const auto it = cur.find(key);
                const bool changed = it == cur.end() || it->second.name != nb.name;
                if (changed && nb.owner != owner_id) {
                    uint16_t dip = rt.pool->find_ip(nb.dip_ip);
                    try {
                        if (dip == kInvalidDip) dip = rt.pool->add("", nb.dip_ip, nullptr, 1);
                    } catch (const std::exception&) {
                        continue;
                    }
                    CoreMsg m{};
                    m.type = CoreMsgType::BindRemote;
                    m.tuple = nb.tuple;
                    m.dip = dip;
                    out.push_back(m);
                }
                if (!live.count(nb.owner)) {
                    const bool kept = it != cur.end() && it->second.owner == nb.owner && it->second.orphan_since_ms != 0;
                    nb.orphan_since_ms = kept ? it->second.orphan_since_ms : now;
                }
            }
            cur.swap(fresh);
            if (!shard_seen[s]) {
                shard_seen[s] = true;
                ++shards_seen;
                cv.notify_all();
            }
        }
        for (const auto& m : out) rt.send_to_owner(m);
    }

    void apply_instances(const String_vector* names) {
        std::unordered_set<std::string> fresh;
        for (int i = 0; i < names->count; ++i) fresh.insert(names->data[i]);
        std::lock_guard lk(mu);
        const uint64_t now = steady_ms();
        for (auto& shard : shards) {
            for (auto& [key, b] : shard) {
                if (fresh.count(b.owner)) {
                    b.orphan_since_ms = 0;
                } else if (b.orphan_since_ms == 0) {
                    b.orphan_since_ms = now;
                }
            }
        }
        live.swap(fresh);
        instances_seen = true;
        cv.notify_all();
    }

    void apply_election(const String_vector* names) {
        std::vector<std::string> nodes;
        for (int i = 0; i < names->count; ++i) nodes.emplace_back(names->data[i]);
        std::sort(nodes.begin(), nodes.end());
        std::lock_guard lk(mu);
        const bool lead = !nodes.empty() && !election_node.empty() && nodes.front() == election_node;
        if (lead != is_leader.exchange(lead) && lead) {
            leadership_changed.store(true);
        }
        election_seen = true;
        cv.notify_all();
    }

    void apply_buckets(const char* value, int len) {
        uint64_t version = 0;
        std::vector<uint16_t> buckets;
        if (value != nullptr && len > 0 && decode_buckets(value, static_cast<size_t>(len), *rt.pool, version, buckets)) {
            hpm.install(version, std::move(buckets));
            std::lock_guard lk(mu);
            buckets_exist = true;
        }
        std::lock_guard lk(mu);
        buckets_seen = true;
        cv.notify_all();
    }

    void buckets_absent() {
        zoo_awexists(zh.load(), buckets_path.c_str(), buckets_watcher, this, buckets_exists, this);
        std::lock_guard lk(mu);
        buckets_seen = true;
        cv.notify_all();
    }

    // --- writes -----------------------------------------------------------------

    void release_held(uint64_t id, bool timed_out) {
        if (id == 0) return;
        Held h;
        {
            std::lock_guard lk(held_mu);
            const auto it = held.find(id);
            if (it == held.end()) return;
            h = it->second;
            held.erase(it);
        }
        if (timed_out) timeouts.fetch_add(1);
        if (rt.release_rings[h.worker] == nullptr || rte_ring_mp_enqueue(rt.release_rings[h.worker], h.m) != 0) {
            rte_pktmbuf_free(h.m);
            return;
        }
        released.fetch_add(1);
    }

    void expire_held(uint64_t now) {
        std::vector<uint64_t> due;
        {
            std::lock_guard lk(held_mu);
            for (const auto& [id, h] : held) {
                if (h.deadline_ms > now) break;
                due.push_back(id);
            }
        }
        for (uint64_t id : due) release_held(id, true);
    }

    void create_binding(const FiveTuple& t, uint32_t dip_ip, rte_mbuf* m, unsigned worker) {
        uint64_t id = 0;
        if (m != nullptr) {
            std::lock_guard lk(held_mu);
            id = ++next_held_id;
            held[id] = Held{worker, m, steady_ms() + rt.cfg.zk_write_timeout_ms};
            held_count.fetch_add(1);
        }
        zhandle_t* h = zh.load();
        if (h == nullptr || !is_connected()) {
            fail_open.fetch_add(1);
            release_held(id, false);
            return;
        }
        auto* op = new OpCtx{this, id};
        const std::string path = binding_path(t, dip_ip, current_owner());
        if (zoo_acreate(h, path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0, create_done, op) != ZOK) {
            delete op;
            fail_open.fetch_add(1);
            release_held(id, false);
            return;
        }
        creates.fetch_add(1);
    }

    void delete_binding(const FiveTuple& t, uint32_t dip_ip) {
        zhandle_t* h = zh.load();
        if (h == nullptr) return;
        const std::string path = binding_path(t, dip_ip, current_owner());
        if (zoo_adelete(h, path.c_str(), -1, delete_done, this) == ZOK) deletes.fetch_add(1);
    }

    void adopt_binding(const FiveTuple& t, uint32_t dip_ip) {
        const std::string key = tuple_to_hex(t);
        const int s = shard_of(t);
        std::string old_name;
        std::string owner;
        {
            std::lock_guard lk(mu);
            owner = owner_id;
            const auto it = shards[s].find(key);
            if (it != shards[s].end()) {
                if (it->second.owner == owner_id) return;
                old_name = it->second.name;
            }
        }
        zhandle_t* h = zh.load();
        if (h == nullptr) return;
        if (old_name.empty()) {
            create_binding(t, dip_ip, nullptr, 0);
            return;
        }
        auto* ctx = new AdoptCtx();
        ctx->self = this;
        ctx->old_path = bindings_path + "/" + shard_name(s) + "/" + old_name;
        ctx->new_path = binding_path(t, dip_ip, owner);
        zoo_delete_op_init(&ctx->ops[0], ctx->old_path.c_str(), -1);
        zoo_create_op_init(&ctx->ops[1], ctx->new_path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                           ctx->path_buf, sizeof(ctx->path_buf));
        if (zoo_amulti(h, 2, ctx->ops, ctx->results, adopt_done, ctx) != ZOK) {
            delete ctx;
            return;
        }
        adoptions.fetch_add(1);
    }

    void on_adopt_done(AdoptCtx* ctx, int rc) {
        if (rc != ZOK) {
            // The old binding vanished meanwhile; record ours directly.
            if (zhandle_t* h = zh.load()) {
                zoo_acreate(h, ctx->new_path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0, create_done,
                            new OpCtx{this, 0});
            }
        }
        delete ctx;
    }

    void publish_buckets() {
        if (rt.cfg.hashing != HashingScheme::Stable) return;
        zhandle_t* h = zh.load();
        if (h == nullptr) return;
        const auto [version, buckets] = hpm.current();
        const std::string data = encode_buckets(version, buckets, *rt.pool);
        int rc = zoo_set(h, buckets_path.c_str(), data.data(), static_cast<int>(data.size()), -1);
        if (rc == ZNONODE) {
            rc = zoo_create(h, buckets_path.c_str(), data.data(), static_cast<int>(data.size()), &ZOO_OPEN_ACL_UNSAFE,
                            0, nullptr, 0);
            if (rc == ZNODEEXISTS) {
                rc = zoo_set(h, buckets_path.c_str(), data.data(), static_cast<int>(data.size()), -1);
            }
        }
        if (rc != ZOK) {
            std::cerr << "[coordinator] cannot publish the bucket table: " << zerror(rc) << std::endl;
            return;
        }
        bucket_publishes.fetch_add(1);
    }

    void collect_garbage() {
        if (!is_leader.load()) return;
        const uint64_t now = steady_ms();
        std::vector<std::string> doomed;
        {
            std::lock_guard lk(mu);
            for (int s = 0; s < kShards; ++s) {
                for (const auto& [key, b] : shards[s]) {
                    if (live.count(b.owner)) continue;
                    const uint16_t dip = rt.pool->find_ip(b.dip_ip);
                    const bool dip_gone = dip == kInvalidDip || !(*rt.pool)[dip].eligible.load(std::memory_order_relaxed);
                    const bool stale = b.orphan_since_ms != 0 && now - b.orphan_since_ms >= orphan_timeout_ms;
                    if (dip_gone || stale) {
                        doomed.push_back(bindings_path + "/" + shard_name(s) + "/" + b.name);
                    }
                }
            }
        }
        zhandle_t* h = zh.load();
        if (h == nullptr) return;
        for (const auto& path : doomed) {
            if (zoo_adelete(h, path.c_str(), -1, delete_done, this) == ZOK) gc_deletes.fetch_add(1);
        }
    }

    // --- coordination thread ----------------------------------------------------

    void run() {
        ZkReq reqs[64];
        while (running.load()) {
            bool need_reconnect;
            {
                std::lock_guard lk(mu);
                need_reconnect = expired;
            }
            if (need_reconnect) {
                reconnect();
            }

            const unsigned n = rte_ring_sc_dequeue_burst_elem(rt.zk_requests, reqs, sizeof(ZkReq), 64, nullptr);
            for (unsigned i = 0; i < n; ++i) {
                const ZkReq& r = reqs[i];
                switch (r.type) {
                case ZkReqType::Create: create_binding(r.tuple, r.dip_ip_be, r.held, r.worker); break;
                case ZkReqType::Delete: delete_binding(r.tuple, r.dip_ip_be); break;
                case ZkReqType::Adopt: adopt_binding(r.tuple, r.dip_ip_be); break;
                }
            }

            const uint64_t now = steady_ms();
            expire_held(now);
            if (leadership_changed.exchange(false) && is_leader.load()) {
                hpm.rebuild(eligible_fn());
                publish_buckets();
            }
            if (now - last_gc_ms.load() >= kGcPeriodMs) {
                last_gc_ms.store(now);
                collect_garbage();
            }
            if (n == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }
    }
};

namespace {

void session_watcher(zhandle_t*, int type, int state, const char*, void* ctx) {
    if (type == ZOO_SESSION_EVENT && ctx != nullptr) {
        static_cast<Coordinator::Impl*>(ctx)->on_session_event(state);
    }
}

void shard_watcher(zhandle_t*, int type, int, const char*, void* ctx) {
    if (type == ZOO_CHILD_EVENT) {
        auto* c = static_cast<Coordinator::Impl::ShardCtx*>(ctx);
        c->self->watch_shard(c->shard);
    }
}

void shard_children(int rc, const String_vector* names, const void* data) {
    auto* c = static_cast<const Coordinator::Impl::ShardCtx*>(data);
    if (rc == ZOK && names != nullptr) {
        c->self->apply_shard(c->shard, names);
    }
}

void instances_watcher(zhandle_t*, int type, int, const char*, void* ctx) {
    if (type == ZOO_CHILD_EVENT) static_cast<Coordinator::Impl*>(ctx)->watch_instances();
}

void instances_children(int rc, const String_vector* names, const void* data) {
    if (rc == ZOK && names != nullptr) {
        static_cast<Coordinator::Impl*>(const_cast<void*>(data))->apply_instances(names);
    }
}

void election_watcher(zhandle_t*, int type, int, const char*, void* ctx) {
    if (type == ZOO_CHILD_EVENT) static_cast<Coordinator::Impl*>(ctx)->watch_election();
}

void election_children(int rc, const String_vector* names, const void* data) {
    if (rc == ZOK && names != nullptr) {
        static_cast<Coordinator::Impl*>(const_cast<void*>(data))->apply_election(names);
    }
}

void buckets_watcher(zhandle_t*, int type, int, const char*, void* ctx) {
    if (type == ZOO_CHANGED_EVENT || type == ZOO_CREATED_EVENT || type == ZOO_DELETED_EVENT) {
        static_cast<Coordinator::Impl*>(ctx)->watch_buckets();
    }
}

void buckets_data(int rc, const char* value, int len, const Stat*, const void* data) {
    auto* self = static_cast<Coordinator::Impl*>(const_cast<void*>(data));
    if (rc == ZOK) {
        self->apply_buckets(value, len);
    } else if (rc == ZNONODE) {
        self->buckets_absent();
    }
}

void buckets_exists(int rc, const Stat*, const void* data) {
    if (rc == ZOK) {
        static_cast<Coordinator::Impl*>(const_cast<void*>(data))->watch_buckets();
    }
}

void create_done(int rc, const char*, const void* data) {
    auto* op = static_cast<Coordinator::Impl::OpCtx*>(const_cast<void*>(data));
    if (rc != ZOK && rc != ZNODEEXISTS) {
        op->self->create_errors.fetch_add(1);
    }
    op->self->release_held(op->held_id, false);
    delete op;
}

void delete_done(int, const void*) {}

void adopt_done(int rc, const void* data) {
    auto* ctx = static_cast<Coordinator::Impl::AdoptCtx*>(const_cast<void*>(data));
    ctx->self->on_adopt_done(ctx, rc);
}

} // namespace

Coordinator::Coordinator(Runtime& rt, HashPathManager& hpm, std::function<std::vector<uint16_t>()> eligible)
    : impl_(std::make_unique<Impl>(rt, hpm, std::move(eligible))) {}

Coordinator::~Coordinator() {
    stop();
}

bool Coordinator::start() {
    Impl& s = *impl_;
    const auto t0 = std::chrono::steady_clock::now();
    if (!s.connect(s.rt.cfg.zk_sync_timeout_ms) || !s.register_instance()) {
        return false;
    }
    s.watch_all();
    if (!s.wait_synced(s.rt.cfg.zk_sync_timeout_ms)) {
        std::cerr << "[coordinator] timed out loading the shared state" << std::endl;
        return false;
    }
    bool publish;
    {
        std::lock_guard lk(s.mu);
        publish = s.is_leader.load() && !s.buckets_exist;
    }
    if (publish) {
        s.publish_buckets();
    }
    // The local copy is complete once the forwarding cores have consumed every binding.
    for (unsigned w = 0; w < s.rt.nworkers; ++w) {
        while (rte_ring_count(s.rt.ctrl_rings[w]) != 0 &&
               std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(s.rt.cfg.zk_sync_timeout_ms)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    s.sync_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
    s.synced.store(true);
    s.running.store(true);
    s.thread = std::thread([&s] { s.run(); });
    std::cout << "[coordinator] synchronized " << stats().bindings << " bindings in " << s.sync_ms << " ms as "
              << stats().owner_id << (s.is_leader.load() ? " (leader)" : "") << std::endl;
    return true;
}

void Coordinator::stop() {
    Impl& s = *impl_;
    if (s.running.exchange(false) && s.thread.joinable()) {
        s.thread.join();
    }
    {
        std::lock_guard lk(s.held_mu);
        for (auto& [id, h] : s.held) rte_pktmbuf_free(h.m);
        s.held.clear();
    }
    if (zhandle_t* h = s.zh.exchange(nullptr)) {
        zookeeper_close(h);
    }
}

bool Coordinator::leader() const {
    return impl_->is_leader.load();
}

void Coordinator::publish_buckets() {
    impl_->publish_buckets();
}

void Coordinator::on_dips_removed(const std::vector<uint16_t>& removed) {
    if (!removed.empty()) impl_->last_gc_ms.store(0);
}

Coordinator::Stats Coordinator::stats() const {
    const Impl& s = *impl_;
    Stats st;
    {
        std::lock_guard lk(s.mu);
        st.connected = s.connected;
        st.owner_id = s.owner_id;
        st.live_instances = s.live.size();
        for (const auto& shard : s.shards) {
            st.bindings += shard.size();
            for (const auto& [key, b] : shard) {
                if (!s.live.count(b.owner)) ++st.orphans;
            }
        }
    }
    st.leader = s.is_leader.load();
    st.synced = s.synced.load();
    st.sync_ms = s.sync_ms;
    st.creates = s.creates.load();
    st.create_errors = s.create_errors.load();
    st.deletes = s.deletes.load();
    st.adoptions = s.adoptions.load();
    st.held = s.held_count.load();
    st.released = s.released.load();
    st.timeouts = s.timeouts.load();
    st.fail_open = s.fail_open.load();
    st.gc_deletes = s.gc_deletes.load();
    st.session_restarts = s.session_restarts.load();
    st.bucket_publishes = s.bucket_publishes.load();
    return st;
}

} // namespace balancify::lb

#else // !BALANCIFY_HAS_ZOOKEEPER

namespace balancify::lb {

struct Coordinator::Impl {};

Coordinator::Coordinator(Runtime&, HashPathManager&, std::function<std::vector<uint16_t>()>)
    : impl_(std::make_unique<Impl>()) {}

Coordinator::~Coordinator() = default;

bool Coordinator::start() {
    std::cerr << "[coordinator] built without ZooKeeper support; rebuild with -DBALANCIFY_WITH_ZOOKEEPER=ON" << std::endl;
    return false;
}

void Coordinator::stop() {}
bool Coordinator::leader() const { return true; }
void Coordinator::publish_buckets() {}
void Coordinator::on_dips_removed(const std::vector<uint16_t>&) {}
Coordinator::Stats Coordinator::stats() const { return {}; }

} // namespace balancify::lb

#endif
