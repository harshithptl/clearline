#include "book.hpp"

namespace clearline {

namespace {

// Largest integer a double represents exactly. Tick prices stay below it, so
// the engine's double prices are exact whole numbers.
constexpr int64_t kMaxExactTicks = int64_t{1} << 53;
constexpr int32_t kMaxTicksEachSide = 10'000'000;
constexpr uint64_t kMaxOrderId = uint64_t{1} << 28;
constexpr uint32_t kMaxResting = uint32_t{1} << 24;

}  // namespace

std::string Book::validate(const BookConfig& cfg) {
    if (cfg.ticks_each_side < 1 || cfg.ticks_each_side > kMaxTicksEachSide) {
        return "ticks_each_side out of range";
    }
    if (cfg.reference_ticks < 1 || cfg.reference_ticks + cfg.ticks_each_side >= kMaxExactTicks) {
        return "reference_ticks out of range";
    }
    if (cfg.max_order_id < 1 || cfg.max_order_id > kMaxOrderId) {
        return "max_order_id out of range";
    }
    if (cfg.max_resting < 1 || cfg.max_resting > kMaxResting) {
        return "max_resting out of range";
    }
    return "";
}

// The ring must hold every order recycled by one submit() without ever
// filling up: OrderBook spins on a full ring, and nothing else would drain
// it. One call recycles at most max_resting orders, and a ring of capacity c
// holds c - 1 items.
size_t Book::ring_capacity_for(uint32_t max_resting) {
    size_t cap = 2;
    while (cap < static_cast<size_t>(max_resting) + 2) {
        cap <<= 1;
    }
    return cap;
}

Book::Book(const BookConfig& cfg)
    : cfg_(cfg),
      slab_(cfg.max_resting),
      recycle_ring_(ring_capacity_for(cfg.max_resting)),
      book_(static_cast<double>(cfg.reference_ticks), 1.0, cfg.ticks_each_side,
            static_cast<size_t>(cfg.max_order_id), recycle_ring_) {}

void Book::drain_recycles() {
    llb::Order* ord = nullptr;
    while (recycle_ring_.try_pop(ord)) {
        slab_.deallocate(ord);
    }
}

SubmitOutcome Book::submit(uint64_t order_id, char side, int64_t price_ticks, uint32_t quantity,
                           std::vector<llb::Fill>& fills) {
    fills.clear();
    if ((side != 'B' && side != 'S') || quantity == 0 || order_id >= cfg_.max_order_id ||
        (has_last_id_ && order_id <= last_id_)) {
        return SubmitOutcome{SubmitStatus::RejectedInvalid, 0};
    }
    const double price = static_cast<double>(price_ticks);
    if (price_ticks < 1 || price_ticks >= kMaxExactTicks || !book_.accepts_price(price)) {
        return SubmitOutcome{SubmitStatus::RejectedPrice, 0};
    }
    llb::Order* ord = slab_.create_order(order_id, price, quantity, side);
    if (ord == nullptr) {
        return SubmitOutcome{SubmitStatus::RejectedCapacity, 0};
    }
    has_last_id_ = true;
    last_id_ = order_id;

    book_.submit(ord, &fills);
    // Read before draining: a fully filled order is in the recycle ring with
    // quantity 0, and draining hands its slot back to the slab.
    const uint32_t resting = ord->quantity;
    drain_recycles();
    return SubmitOutcome{SubmitStatus::Accepted, resting};
}

}  // namespace clearline
