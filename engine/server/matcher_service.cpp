#include "matcher_service.hpp"

#include <cmath>
#include <cstdio>

namespace clearline {

namespace mv1 = matcher::v1;

MatcherService::MatcherService(size_t max_sessions)
    : max_sessions_(max_sessions < 1 ? 1 : max_sessions), rng_(std::random_device{}()) {}

size_t MatcherService::session_count() {
    std::lock_guard<std::mutex> lock(mu_);
    return sessions_.size();
}

std::string MatcherService::new_session_id_locked() {
    for (;;) {
        char buf[33];
        std::snprintf(buf, sizeof(buf), "%016llx%016llx", static_cast<unsigned long long>(rng_()),
                      static_cast<unsigned long long>(rng_()));
        std::string id(buf);
        if (sessions_.find(id) == sessions_.end()) {
            return id;
        }
    }
}

void MatcherService::evict_lru_locked() {
    auto oldest = sessions_.end();
    for (auto it = sessions_.begin(); it != sessions_.end(); ++it) {
        if (oldest == sessions_.end() || it->second.last_used < oldest->second.last_used) {
            oldest = it;
        }
    }
    if (oldest != sessions_.end()) {
        sessions_.erase(oldest);
    }
}

grpc::Status MatcherService::OpenSession(grpc::ServerContext*, const mv1::OpenSessionRequest* req,
                                         mv1::OpenSessionResponse* resp) {
    if (req->books_size() == 0) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "no books");
    }
    // Validate everything before allocating anything.
    std::map<std::string, BookConfig> configs;
    for (const mv1::BookSpec& spec : req->books()) {
        if (spec.symbol().empty()) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty symbol");
        }
        BookConfig cfg{spec.reference_ticks(), spec.ticks_each_side(), spec.max_order_id(),
                       spec.max_resting()};
        const std::string err = Book::validate(cfg);
        if (!err.empty()) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, spec.symbol() + ": " + err);
        }
        if (!configs.emplace(spec.symbol(), cfg).second) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "duplicate symbol " + spec.symbol());
        }
    }

    std::lock_guard<std::mutex> lock(mu_);
    while (sessions_.size() >= max_sessions_) {
        evict_lru_locked();
    }
    Session session;
    for (const auto& entry : configs) {
        session.books.emplace(entry.first, std::make_unique<Book>(entry.second));
    }
    session.last_used = ++clock_;
    const std::string id = new_session_id_locked();
    sessions_.emplace(id, std::move(session));
    resp->set_session(id);
    return grpc::Status::OK;
}

grpc::Status MatcherService::CloseSession(grpc::ServerContext*, const mv1::CloseSessionRequest* req,
                                          mv1::CloseSessionResponse*) {
    std::lock_guard<std::mutex> lock(mu_);
    sessions_.erase(req->session());
    return grpc::Status::OK;
}

mv1::Status MatcherService::to_proto(SubmitStatus s) {
    switch (s) {
        case SubmitStatus::Accepted:
            return mv1::STATUS_ACCEPTED;
        case SubmitStatus::RejectedPrice:
            return mv1::STATUS_REJECTED_PRICE;
        case SubmitStatus::RejectedCapacity:
            return mv1::STATUS_REJECTED_CAPACITY;
        case SubmitStatus::RejectedInvalid:
            return mv1::STATUS_REJECTED_INVALID;
    }
    return mv1::STATUS_UNSPECIFIED;
}

void MatcherService::submit_one_locked(Session& session, const mv1::Order& order,
                                       mv1::OrderResult* result) {
    auto it = session.books.find(order.symbol());
    if (it == session.books.end()) {
        result->set_status(mv1::STATUS_REJECTED_UNKNOWN_BOOK);
        return;
    }
    char side = 0;
    if (order.side() == mv1::SIDE_BUY) {
        side = 'B';
    } else if (order.side() == mv1::SIDE_SELL) {
        side = 'S';
    }
    const SubmitOutcome out =
        it->second->submit(order.order_id(), side, order.price_ticks(), order.quantity(), fills_);
    result->set_status(to_proto(out.status));
    result->set_resting_quantity(out.resting_quantity);
    for (const llb::Fill& f : fills_) {
        mv1::Fill* pf = result->add_fills();
        pf->set_maker_order_id(f.maker_id);
        pf->set_price_ticks(std::llround(f.price));
        pf->set_quantity(f.quantity);
    }
}

grpc::Status MatcherService::Submit(grpc::ServerContext*, const mv1::SubmitRequest* req,
                                    mv1::SubmitResponse* resp) {
    if (req->orders_size() > kMaxOrdersPerSubmit) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "too many orders in one request");
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sessions_.find(req->session());
    if (it == sessions_.end()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown session");
    }
    Session& session = it->second;
    session.last_used = ++clock_;
    for (const mv1::Order& order : req->orders()) {
        submit_one_locked(session, order, resp->add_results());
    }
    return grpc::Status::OK;
}

}  // namespace clearline
