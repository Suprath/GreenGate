#pragma once
#include <string>
#include <thread>
#include <atomic>
#include "apex/engine.hpp"
#include "green_gate/local_cache.hpp"

namespace greengate {

class QueryDaemon {
public:
    QueryDaemon(int port, const std::string& cache_dir, 
                const std::string& s3_endpoint, const std::string& bucket);
    ~QueryDaemon();

    void Start();
    void Stop();

private:
    void AcceptLoop();
    void HandleClient(int client_fd);

    int port_;
    int server_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    LocalCache cache_;
    apex::ApexEngine engine_;
};

} // namespace greengate
