#include "book.hpp"

#include <gtest/gtest.h>

#include <random>
#include <vector>

namespace clearline {
namespace {

BookConfig small_config() {
    // Prices 50..150 ticks, ids < 1000, up to 8 resting orders.
    return BookConfig{100, 50, 1000, 8};
}

struct Recorded {
    SubmitStatus status;
    uint32_t resting;
    std::vector<llb::Fill> fills;
};

Recorded submit(Book& book, uint64_t id, char side, int64_t price, uint32_t qty) {
    std::vector<llb::Fill> fills;
    const SubmitOutcome out = book.submit(id, side, price, qty, fills);
    return Recorded{out.status, out.resting_quantity, fills};
}

// SPEC 4.1 in ticks: Bob rests a sell of 10 at 150, Alice buys 4 at 151.
// One fill at the maker's price; Bob keeps 6 resting.
TEST(Book, ReportsFillAtMakerPrice) {
    Book book(small_config());
    Recorded maker = submit(book, 0, 'S', 150, 10);
    ASSERT_EQ(maker.status, SubmitStatus::Accepted);
    EXPECT_EQ(maker.resting, 10u);
    EXPECT_TRUE(maker.fills.empty());

    Recorded taker = submit(book, 1, 'B', 150, 4);
    ASSERT_EQ(taker.status, SubmitStatus::Accepted);
    EXPECT_EQ(taker.resting, 0u);
    ASSERT_EQ(taker.fills.size(), 1u);
    EXPECT_EQ(taker.fills[0].maker_id, 0u);
    EXPECT_EQ(taker.fills[0].price, 150.0);
    EXPECT_EQ(taker.fills[0].quantity, 4u);
    EXPECT_EQ(book.resting_count(), 1u);
}

// Better price first, then earlier order first at the same price.
TEST(Book, PriceTimePriority) {
    Book book(small_config());
    submit(book, 0, 'S', 102, 5);  // worse price
    submit(book, 1, 'S', 101, 3);  // best price, first in time
    submit(book, 2, 'S', 101, 3);  // best price, second in time

    Recorded taker = submit(book, 3, 'B', 102, 8);
    ASSERT_EQ(taker.fills.size(), 3u);
    EXPECT_EQ(taker.fills[0].maker_id, 1u);
    EXPECT_EQ(taker.fills[0].quantity, 3u);
    EXPECT_EQ(taker.fills[1].maker_id, 2u);
    EXPECT_EQ(taker.fills[1].quantity, 3u);
    EXPECT_EQ(taker.fills[2].maker_id, 0u);
    EXPECT_EQ(taker.fills[2].price, 102.0);
    EXPECT_EQ(taker.fills[2].quantity, 2u);
    EXPECT_EQ(taker.resting, 0u);
}

TEST(Book, NoCrossNoFill) {
    Book book(small_config());
    submit(book, 0, 'S', 101, 5);
    Recorded bid = submit(book, 1, 'B', 100, 5);
    EXPECT_TRUE(bid.fills.empty());
    EXPECT_EQ(bid.resting, 5u);
    EXPECT_EQ(book.resting_count(), 2u);
}

TEST(Book, RemainderRests) {
    Book book(small_config());
    submit(book, 0, 'B', 100, 2);
    Recorded sell = submit(book, 1, 'S', 99, 5);
    ASSERT_EQ(sell.fills.size(), 1u);
    EXPECT_EQ(sell.fills[0].price, 100.0);
    EXPECT_EQ(sell.resting, 3u);
}

TEST(Book, Rejections) {
    Book book(small_config());
    EXPECT_EQ(submit(book, 0, 'B', 49, 1).status, SubmitStatus::RejectedPrice);   // below range
    EXPECT_EQ(submit(book, 0, 'B', 151, 1).status, SubmitStatus::RejectedPrice);  // above range
    EXPECT_EQ(submit(book, 0, 'B', 0, 1).status, SubmitStatus::RejectedPrice);
    EXPECT_EQ(submit(book, 0, 'X', 100, 1).status, SubmitStatus::RejectedInvalid);
    EXPECT_EQ(submit(book, 0, 'B', 100, 0).status, SubmitStatus::RejectedInvalid);
    EXPECT_EQ(submit(book, 1000, 'B', 100, 1).status, SubmitStatus::RejectedInvalid);  // id too big

    ASSERT_EQ(submit(book, 5, 'B', 100, 1).status, SubmitStatus::Accepted);
    EXPECT_EQ(submit(book, 5, 'B', 100, 1).status, SubmitStatus::RejectedInvalid);  // reused id
    EXPECT_EQ(submit(book, 4, 'B', 100, 1).status, SubmitStatus::RejectedInvalid);  // went backwards
    EXPECT_EQ(book.resting_count(), 1u);
}

TEST(Book, CapacityAndSlotReuse) {
    Book book(small_config());
    uint64_t id = 0;
    for (int i = 0; i < 8; ++i) {
        ASSERT_EQ(submit(book, id++, 'B', 60 + i, 1).status, SubmitStatus::Accepted);
    }
    EXPECT_EQ(submit(book, id++, 'B', 90, 1).status, SubmitStatus::RejectedCapacity);

    // Slots come back when orders fill: many full cycles on a 2-slot book.
    Book cycle(BookConfig{100, 50, 100000, 2});
    uint64_t cid = 0;
    for (int round = 0; round < 10000; ++round) {
        ASSERT_EQ(submit(cycle, cid++, 'S', 100, 3).status, SubmitStatus::Accepted) << round;
        Recorded t = submit(cycle, cid++, 'B', 100, 3);
        ASSERT_EQ(t.status, SubmitStatus::Accepted) << round;
        ASSERT_EQ(t.fills.size(), 1u);
    }
    EXPECT_EQ(cycle.resting_count(), 0u);
}

// The adapter rebuilds state by replaying orders, so the same input must
// always give the same fills.
TEST(Book, Deterministic) {
    const BookConfig cfg{1000, 100, 100000, 4096};
    Book a(cfg);
    Book b(cfg);
    std::mt19937_64 rng(42);
    size_t total_fills = 0;
    for (uint64_t id = 0; id < 20000; ++id) {
        const char side = (rng() % 2 == 0) ? 'B' : 'S';
        const int64_t price = 950 + static_cast<int64_t>(rng() % 101);
        const uint32_t qty = 1 + static_cast<uint32_t>(rng() % 50);
        Recorded ra = submit(a, id, side, price, qty);
        Recorded rb = submit(b, id, side, price, qty);
        ASSERT_EQ(ra.status, rb.status);
        ASSERT_EQ(ra.resting, rb.resting);
        ASSERT_EQ(ra.fills.size(), rb.fills.size());
        for (size_t i = 0; i < ra.fills.size(); ++i) {
            ASSERT_EQ(ra.fills[i].maker_id, rb.fills[i].maker_id);
            ASSERT_EQ(ra.fills[i].price, rb.fills[i].price);
            ASSERT_EQ(ra.fills[i].quantity, rb.fills[i].quantity);
        }
        total_fills += ra.fills.size();
    }
    EXPECT_GT(total_fills, 1000u);
}

TEST(Book, ValidateConfig) {
    EXPECT_EQ(Book::validate(small_config()), "");
    EXPECT_NE(Book::validate(BookConfig{100, 0, 10, 1}), "");
    EXPECT_NE(Book::validate(BookConfig{0, 5, 10, 1}), "");
    EXPECT_NE(Book::validate(BookConfig{100, 5, 0, 1}), "");
    EXPECT_NE(Book::validate(BookConfig{100, 5, 10, 0}), "");
}

}  // namespace
}  // namespace clearline
