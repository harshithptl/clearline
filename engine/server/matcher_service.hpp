#pragma once
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include "book.hpp"
#include "matcher/v1/matcher.grpc.pb.h"

namespace clearline {

// gRPC Matcher service. All state sits behind one mutex: llb::OrderBook is
// single-threaded, and the adapter sends one batch at a time anyway.
class MatcherService final : public matcher::v1::Matcher::Service {
public:
    static constexpr int kMaxOrdersPerSubmit = 10000;

    explicit MatcherService(size_t max_sessions);

    grpc::Status OpenSession(grpc::ServerContext* ctx, const matcher::v1::OpenSessionRequest* req,
                             matcher::v1::OpenSessionResponse* resp) override;
    grpc::Status CloseSession(grpc::ServerContext* ctx, const matcher::v1::CloseSessionRequest* req,
                              matcher::v1::CloseSessionResponse* resp) override;
    grpc::Status Submit(grpc::ServerContext* ctx, const matcher::v1::SubmitRequest* req,
                        matcher::v1::SubmitResponse* resp) override;

    size_t session_count();

private:
    struct Session {
        std::map<std::string, std::unique_ptr<Book>> books;
        uint64_t last_used = 0;
    };

    static matcher::v1::Status to_proto(SubmitStatus s);
    std::string new_session_id_locked();
    void evict_lru_locked();
    void submit_one_locked(Session& session, const matcher::v1::Order& order,
                           matcher::v1::OrderResult* result);

    std::mutex mu_;
    std::map<std::string, Session> sessions_;
    const size_t max_sessions_;
    uint64_t clock_ = 0;
    std::mt19937_64 rng_;
    std::vector<llb::Fill> fills_;  // scratch, reused across calls
};

}  // namespace clearline
