#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "llb/order_book.hpp"
#include "llb/slab_allocator.hpp"
#include "llb/spsc_buffer.hpp"

namespace clearline {

// Configuration for one symbol's book. Prices are integer ticks.
struct BookConfig {
    int64_t  reference_ticks;
    int32_t  ticks_each_side;
    uint64_t max_order_id;
    uint32_t max_resting;
};

enum class SubmitStatus {
    Accepted,
    RejectedPrice,
    RejectedCapacity,
    RejectedInvalid,
};

struct SubmitOutcome {
    SubmitStatus status;
    uint32_t     resting_quantity;
};

// Book wraps llb::OrderBook for single-threaded use behind the gRPC service:
// it owns the slab and recycle ring, drains recycled orders back into the
// slab after every call, and validates input the engine itself trusts.
//
// Deterministic: the same sequence of submit() calls always produces the same
// fills. The adapter relies on that to rebuild state by replaying orders.
class Book {
public:
    // Returns an empty string if cfg is usable, else the reason it is not.
    static std::string validate(const BookConfig& cfg);

    explicit Book(const BookConfig& cfg);

    Book(const Book&) = delete;
    Book& operator=(const Book&) = delete;

    // side is 'B' or 'S'. fills is cleared, then receives one entry per match.
    // order_id must be < max_order_id and greater than every earlier id.
    // max_resting counts the incoming order too: when every slot is taken,
    // even an order that would fully match is rejected with
    // RejectedCapacity.
    SubmitOutcome submit(uint64_t order_id, char side, int64_t price_ticks, uint32_t quantity,
                         std::vector<llb::Fill>& fills);

    size_t resting_count() const { return book_.resting_count(); }

private:
    static size_t ring_capacity_for(uint32_t max_resting);
    void drain_recycles();

    const BookConfig             cfg_;
    llb::SlabAllocator           slab_;
    llb::SPSCBuffer<llb::Order*> recycle_ring_;
    llb::OrderBook               book_;
    bool                         has_last_id_ = false;
    uint64_t                     last_id_ = 0;
};

}  // namespace clearline
