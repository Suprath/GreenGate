#pragma once

#include <string>
#include <atomic>
#include <cstdint>

namespace greengate {

// Control header at the very beginning of the shared memory page.
// The actual bit-planes array begins immediately after this header.
struct alignas(64) ShmHeader {
    std::atomic<uint64_t> sequence; // Incremented when DPU finishes writing a new block
    std::atomic<bool> exit_flag;    // Set to signal exit to all threads/processes
    uint64_t num_blocks;            // Number of 64-row blocks in the buffer
    uint64_t num_fields;            // Number of columns mapped
};

class ShmBridge {
public:
    // Constructor. If create is true, creates a new POSIX SHM segment;
    // if false, attaches to an existing one.
    ShmBridge(const std::string& shm_name, size_t size_bytes, bool create);
    ~ShmBridge();

    // Returns pointer to mapped shared memory space
    void* GetAddr() const { return addr_; }

private:
    std::string name_;
    int fd_;
    size_t size_;
    void* addr_;
    bool is_creator_;
};

} // namespace greengate
