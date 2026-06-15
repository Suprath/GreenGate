#pragma once
#include <string>
#include <unordered_map>
#include <mutex>

namespace greengate {

struct MappedFile {
    void* addr = nullptr;
    size_t size = 0;
    int fd = -1;
};

class LocalCache {
public:
    LocalCache(const std::string& cache_dir, const std::string& s3_endpoint, const std::string& bucket);
    ~LocalCache();

    // Fetches S3 file to local cache (if missing), mmaps it, and mlocks it.
    MappedFile GetOrDownload(const std::string& s3_key);

    void Release(const std::string& s3_key);

private:
    bool DownloadFile(const std::string& s3_key, const std::string& local_path);

    std::string cache_dir_;
    std::string s3_endpoint_;
    std::string bucket_;
    std::mutex mutex_;
    std::unordered_map<std::string, MappedFile> cache_;
};

} // namespace greengate
