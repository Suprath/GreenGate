#pragma once
#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include "lex/distributed/distributed_coordinator.hpp"

namespace greengate {

class PgWireServer {
public:
    PgWireServer(int port, std::shared_ptr<DistributedCoordinator> coordinator);
    ~PgWireServer();

    void Start();
    void Stop();

private:
    void AcceptLoop();
    void HandleClient(int client_fd);

    int port_;
    std::shared_ptr<DistributedCoordinator> coordinator_;
    int server_fd_ = -1;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
};

} // namespace greengate
