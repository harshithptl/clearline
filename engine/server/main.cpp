// matcher-server: serves the C++ order book over gRPC.
//
//   matcher-server [--addr 0.0.0.0:50051] [--max-sessions 16]
//
// Shuts down cleanly on SIGINT or SIGTERM.
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <pthread.h>
#include <signal.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "matcher_service.hpp"

namespace {

struct Flags {
    std::string addr = "0.0.0.0:50051";
    size_t max_sessions = 16;
};

bool parse_flags(int argc, char** argv, Flags* flags) {
    for (int i = 1; i < argc; ++i) {
        const bool has_value = i + 1 < argc;
        if (std::strcmp(argv[i], "--addr") == 0 && has_value) {
            flags->addr = argv[++i];
        } else if (std::strcmp(argv[i], "--max-sessions") == 0 && has_value) {
            flags->max_sessions = std::strtoul(argv[++i], nullptr, 10);
        } else {
            std::fprintf(stderr, "usage: %s [--addr host:port] [--max-sessions n]\n", argv[0]);
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    Flags flags;
    if (!parse_flags(argc, argv, &flags)) {
        return 2;
    }

    // Block the shutdown signals in every thread (gRPC's threads inherit this
    // mask), then wait for them synchronously below. No async signal handler.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    clearline::MatcherService service(flags.max_sessions);
    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(flags.addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::fprintf(stderr, "matcher-server: failed to listen on %s\n", flags.addr.c_str());
        return 1;
    }
    std::printf("matcher-server listening on %s (max %zu sessions)\n", flags.addr.c_str(),
                flags.max_sessions);
    std::fflush(stdout);

    int sig = 0;
    sigwait(&signals, &sig);
    std::printf("matcher-server: signal %d, shutting down\n", sig);
    server->Shutdown();
    return 0;
}
