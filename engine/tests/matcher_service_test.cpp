#include "matcher_service.hpp"

#include <gtest/gtest.h>

#include <string>

namespace clearline {
namespace {

namespace mv1 = matcher::v1;

mv1::OpenSessionRequest one_book(const std::string& symbol) {
    mv1::OpenSessionRequest req;
    mv1::BookSpec* spec = req.add_books();
    spec->set_symbol(symbol);
    spec->set_reference_ticks(15000);
    spec->set_ticks_each_side(1000);
    spec->set_max_order_id(1000);
    spec->set_max_resting(64);
    return req;
}

std::string open(MatcherService& svc, const mv1::OpenSessionRequest& req) {
    mv1::OpenSessionResponse resp;
    grpc::Status st = svc.OpenSession(nullptr, &req, &resp);
    EXPECT_TRUE(st.ok()) << st.error_message();
    return resp.session();
}

void add_order(mv1::SubmitRequest* req, const std::string& symbol, uint64_t id, mv1::Side side,
               int64_t price, uint32_t qty) {
    mv1::Order* o = req->add_orders();
    o->set_symbol(symbol);
    o->set_order_id(id);
    o->set_side(side);
    o->set_price_ticks(price);
    o->set_quantity(qty);
}

TEST(MatcherService, SubmitReturnsFillsInOrder) {
    MatcherService svc(4);
    const std::string session = open(svc, one_book("AAPL"));

    mv1::SubmitRequest req;
    req.set_session(session);
    add_order(&req, "AAPL", 0, mv1::SIDE_SELL, 15000, 10);
    add_order(&req, "AAPL", 1, mv1::SIDE_BUY, 15001, 4);
    add_order(&req, "MSFT", 2, mv1::SIDE_BUY, 15001, 4);
    add_order(&req, "AAPL", 3, mv1::SIDE_UNSPECIFIED, 15000, 1);
    mv1::SubmitResponse resp;
    ASSERT_TRUE(svc.Submit(nullptr, &req, &resp).ok());

    ASSERT_EQ(resp.results_size(), 4);
    EXPECT_EQ(resp.results(0).status(), mv1::STATUS_ACCEPTED);
    EXPECT_EQ(resp.results(0).resting_quantity(), 10u);
    EXPECT_EQ(resp.results(1).status(), mv1::STATUS_ACCEPTED);
    ASSERT_EQ(resp.results(1).fills_size(), 1);
    EXPECT_EQ(resp.results(1).fills(0).maker_order_id(), 0u);
    EXPECT_EQ(resp.results(1).fills(0).price_ticks(), 15000);
    EXPECT_EQ(resp.results(1).fills(0).quantity(), 4u);
    EXPECT_EQ(resp.results(2).status(), mv1::STATUS_REJECTED_UNKNOWN_BOOK);
    EXPECT_EQ(resp.results(3).status(), mv1::STATUS_REJECTED_INVALID);
}

TEST(MatcherService, UnknownSessionIsNotFound) {
    MatcherService svc(4);
    mv1::SubmitRequest req;
    req.set_session("nope");
    mv1::SubmitResponse resp;
    EXPECT_EQ(svc.Submit(nullptr, &req, &resp).error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST(MatcherService, SessionsAreIsolated) {
    MatcherService svc(4);
    const std::string a = open(svc, one_book("AAPL"));
    const std::string b = open(svc, one_book("AAPL"));
    ASSERT_NE(a, b);

    mv1::SubmitRequest rest;
    rest.set_session(a);
    add_order(&rest, "AAPL", 0, mv1::SIDE_SELL, 15000, 5);
    mv1::SubmitResponse r1;
    ASSERT_TRUE(svc.Submit(nullptr, &rest, &r1).ok());

    // Same crossing buy in session b finds nothing to match.
    mv1::SubmitRequest buy;
    buy.set_session(b);
    add_order(&buy, "AAPL", 0, mv1::SIDE_BUY, 15000, 5);
    mv1::SubmitResponse r2;
    ASSERT_TRUE(svc.Submit(nullptr, &buy, &r2).ok());
    EXPECT_EQ(r2.results(0).fills_size(), 0);
    EXPECT_EQ(r2.results(0).resting_quantity(), 5u);
}

TEST(MatcherService, EvictsLeastRecentlyUsedAndCloses) {
    MatcherService svc(2);
    const std::string a = open(svc, one_book("AAPL"));
    const std::string b = open(svc, one_book("AAPL"));

    // Touch a, so b is least recently used.
    mv1::SubmitRequest touch;
    touch.set_session(a);
    mv1::SubmitResponse ignored;
    ASSERT_TRUE(svc.Submit(nullptr, &touch, &ignored).ok());

    const std::string c = open(svc, one_book("AAPL"));
    EXPECT_EQ(svc.session_count(), 2u);
    mv1::SubmitRequest probe;
    mv1::SubmitResponse resp;
    probe.set_session(b);
    EXPECT_EQ(svc.Submit(nullptr, &probe, &resp).error_code(), grpc::StatusCode::NOT_FOUND);
    probe.set_session(a);
    EXPECT_TRUE(svc.Submit(nullptr, &probe, &resp).ok());

    mv1::CloseSessionRequest close;
    close.set_session(c);
    mv1::CloseSessionResponse closed;
    ASSERT_TRUE(svc.CloseSession(nullptr, &close, &closed).ok());
    EXPECT_EQ(svc.session_count(), 1u);
}

TEST(MatcherService, RejectsBadSpecs) {
    MatcherService svc(4);
    mv1::OpenSessionResponse resp;
    mv1::OpenSessionRequest empty;
    EXPECT_EQ(svc.OpenSession(nullptr, &empty, &resp).error_code(),
              grpc::StatusCode::INVALID_ARGUMENT);

    mv1::OpenSessionRequest dup = one_book("AAPL");
    *dup.add_books() = dup.books(0);
    EXPECT_EQ(svc.OpenSession(nullptr, &dup, &resp).error_code(),
              grpc::StatusCode::INVALID_ARGUMENT);

    mv1::OpenSessionRequest bad = one_book("AAPL");
    bad.mutable_books(0)->set_ticks_each_side(0);
    EXPECT_EQ(svc.OpenSession(nullptr, &bad, &resp).error_code(),
              grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(svc.session_count(), 0u);
}

}  // namespace
}  // namespace clearline
