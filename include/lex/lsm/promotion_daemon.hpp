#pragma once
#include "lex/lsm/metadata_registry.hpp"
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>

namespace greengate {

class PromotionDaemon {
public:
    PromotionDaemon(const std::string& table_name, size_t memory_threshold_bytes);
    ~PromotionDaemon();

    void Start();
    void Stop();

    // Trigger compaction immediately (synchronous for testing/daemon loop)
    void TriggerCompaction();

private:
    void DaemonLoop();

    std::string table_name_;
    size_t memory_threshold_;
    std::thread worker_thread_;
    std::atomic<bool> running_{false};
    std::condition_variable cv_;
    std::mutex mutex_;
};

} // namespace greengate
