#include "green_gate/shm_bridge.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <string>

namespace greengate {

ShmBridge::ShmBridge(const std::string& shm_name, size_t size_bytes, bool create)
    : name_(shm_name), fd_(-1), size_(size_bytes), addr_(MAP_FAILED), is_creator_(create) {
    
    // Ensure the shm name starts with a single slash
    std::string path = shm_name;
    if (path.empty() || path[0] != '/') {
        path = "/" + path;
    }
    name_ = path;

    if (create) {
        // Create new SHM segment with 0600 permissions (Owner-only Read/Write)
        // to comply with strict security and prevent local data leaks.
        fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd_ < 0) {
            throw std::runtime_error("shm_open create failed: " + std::string(strerror(errno)));
        }

        // Allocate file size
        if (ftruncate(fd_, size_) != 0) {
            close(fd_);
            shm_unlink(name_.c_str());
            throw std::runtime_error("ftruncate failed: " + std::string(strerror(errno)));
        }

        // Map shared memory space (Read/Write)
        addr_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (addr_ == MAP_FAILED) {
            close(fd_);
            shm_unlink(name_.c_str());
            throw std::runtime_error("mmap failed: " + std::string(strerror(errno)));
        }

        // Zero out the shared memory page to prevent security leaks of old bytes
        std::memset(addr_, 0, size_);
    } else {
        // Attach to existing SHM segment
        fd_ = shm_open(name_.c_str(), O_RDWR, 0);
        if (fd_ < 0) {
            throw std::runtime_error("shm_open attach failed: " + std::string(strerror(errno)));
        }

        // Map shared memory space
        addr_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (addr_ == MAP_FAILED) {
            close(fd_);
            throw std::runtime_error("mmap attach failed: " + std::string(strerror(errno)));
        }
    }
}

ShmBridge::~ShmBridge() {
    if (addr_ != MAP_FAILED) {
        munmap(addr_, size_);
    }
    if (fd_ >= 0) {
        close(fd_);
    }
    if (is_creator_) {
        shm_unlink(name_.c_str());
    }
}

} // namespace greengate
