#pragma once

#include "snapshots.hpp"
#include "types.hpp"

#include <limits>

namespace balancify::lb {

struct OverrideDecision {
    uint16_t dip{kInvalidDip};
    bool pinned{false};  // true: record the connection in the connection-to-DIP table
};

inline uint16_t pick_target(const LoadSnapshot& s, TargetRule rule, PendingLoad& pending, uint64_t rnd) {
    switch (rule) {
    case TargetRule::ArgMin:
        return s.argmin;
    case TargetRule::RandomBelowAverage:
        return s.n_below != 0 ? s.below[rnd % s.n_below] : s.argmin;
    case TargetRule::IncrementTarget: {
        uint16_t best = kInvalidDip;
        double best_load = std::numeric_limits<double>::max();
        for (uint16_t i = 0; i < s.ndips; ++i) {
            if (!s.eligible[i]) continue;
            const double l = s.load[i] + pending.get(i);
            if (l < best_load) {
                best_load = l;
                best = i;
            }
        }
        if (best != kInvalidDip) {
            pending.add(best, s.mu);
        }
        return best;
    }
    }
    return s.argmin;
}

// Decision for the first packet of a new connection (Algorithm 2, lines 3-7).
// `hashed` is the DIP selected by consistent hashing.
inline OverrideDecision decide(uint16_t hashed, const LoadSnapshot* s, Mode mode, double threshold,
                               TargetRule rule, PendingLoad& pending, uint64_t rnd) {
    if (mode == Mode::Stateless) {
        return {hashed, false};
    }
    if (s == nullptr || s->n_eligible == 0) {
        return {hashed, mode == Mode::AllStateful && hashed != kInvalidDip};
    }
    if (mode == Mode::Balancify && hashed < s->ndips && s->eligible[hashed]) {
        const double hashed_load = s->load[hashed] + pending.get(hashed);
        const double avg = s->avg + pending.total_value() / s->n_eligible;
        if (hashed_load - avg <= threshold) {
            return {hashed, false};
        }
    }
    const uint16_t target = pick_target(*s, rule, pending, rnd);
    if (target == kInvalidDip) {
        return {hashed, false};
    }
    return {target, true};
}

} // namespace balancify::lb
