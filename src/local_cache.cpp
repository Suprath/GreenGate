#include "green_gate/local_cache.hpp"
#include <curl/curl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <cstring>
#include <cerrno>

namespace greengate {

LocalCache::LocalCache(const std::string& cache_dir, const std::string& s3_endpoint, const std::string& bucket)
    : cache_dir_(cache_dir), s3_endpoint_(s3_endpoint), bucket_(bucket) {
    std::filesystem::create_directories(cache_dir_);
}

LocalCache::~LocalCache() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& [key, mapped] : cache_) {
        if (mapped.addr) {
            munlock(mapped.addr, mapped.size);
            munmap(mapped.addr, mapped.size);
        }
        if (mapped.fd != -1) {
            close(mapped.fd);
        }
    }
}

static size_t WriteFileCallback(void* ptr, size_t size, size_t nmemb, void* stream) {
    auto* out_file = static_cast<std::ofstream*>(stream);
    out_file->write(static_cast<const char*>(ptr), size * nmemb);
    return size * nmemb;
}

static std::string GetS3Url(const std::string& endpoint, const std::string& bucket, const std::string& key) {
    size_t suffix_pos = bucket.find("--");
    if (suffix_pos != std::string::npos && bucket.rfind("--x-s3") == bucket.size() - 6) {
        std::string az_id = bucket.substr(suffix_pos + 2, bucket.size() - suffix_pos - 8);
        std::string region = "us-east-1";
        if (az_id.compare(0, 4, "use1") == 0) region = "us-east-1";
        else if (az_id.compare(0, 4, "use2") == 0) region = "us-east-2";
        else if (az_id.compare(0, 4, "usw1") == 0) region = "us-west-1";
        else if (az_id.compare(0, 4, "usw2") == 0) region = "us-west-2";
        
        if (endpoint.find("amazonaws.com") != std::string::npos) {
            return "https://" + bucket + ".s3express-" + az_id + "." + region + ".amazonaws.com/" + key;
        }
    }
    
    std::string url = endpoint;
    if (url.back() != '/') url += '/';
    url += bucket + "/" + key;
    return url;
}

bool LocalCache::DownloadFile(const std::string& s3_key, const std::string& local_path) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    std::string url = GetS3Url(s3_endpoint_, bucket_, s3_key);

    std::ofstream out_file(local_path, std::ios::binary);
    if (!out_file.is_open()) {
        std::cerr << "❌ Failed to open local file for downloading: " << local_path << "\n";
        curl_easy_cleanup(curl);
        return false;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out_file);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteFileCallback);
    
    // Local dev: skip SSL checks if testing with local HTTPS MinIO
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    CURLcode res = curl_easy_perform(curl);
    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    curl_easy_cleanup(curl);
    out_file.close();

    if (res != CURLE_OK) {
        std::cerr << "❌ S3 download failed: " << curl_easy_strerror(res) << "\n";
        std::filesystem::remove(local_path);
        return false;
    }

    if (response_code < 200 || response_code >= 300) {
        std::cerr << "❌ S3 download failed with HTTP response code " << response_code << "\n";
        std::filesystem::remove(local_path);
        return false;
    }

    return true;
}

MappedFile LocalCache::GetOrDownload(const std::string& s3_key) {
    std::unique_lock<std::mutex> lock(mutex_);

    // Check memory cache first
    auto it = cache_.find(s3_key);
    if (it != cache_.end()) {
        return it->second;
    }

    std::string local_path = cache_dir_ + "/" + s3_key;
    if (!std::filesystem::exists(local_path)) {
        // Download from S3/MinIO
        if (!DownloadFile(s3_key, local_path)) {
            throw std::runtime_error("Failed to download AGB file from S3: " + s3_key);
        }
    }

    // Open local file
    int fd = open(local_path.c_str(), O_RDONLY);
    if (fd == -1) {
        throw std::runtime_error("Failed to open local cached AGB file: " + local_path + " (" + std::strerror(errno) + ")");
    }

    // Get file size
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        throw std::runtime_error("Failed to stat cached AGB file: " + local_path + " (" + std::strerror(errno) + ")");
    }
    size_t size = sb.st_size;

    // Memory map the file
    void* addr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to mmap AGB file: " + local_path + " (" + std::strerror(errno) + ")");
    }

    // Pin memory using mlock
    if (mlock(addr, size) == -1) {
        std::cerr << "⚠️  mlock failed for " << s3_key << " (" << std::strerror(errno) 
                  << "). Continuing without RAM pinning. Ensure IPC_LOCK privilege is enabled.\n";
    } else {
        std::cout << "✓ RAM Pinned: Successfully mlocked " << (size / 1024) << " KB for " << s3_key << "\n";
    }

    MappedFile mapped{addr, size, fd};
    cache_[s3_key] = mapped;
    return mapped;
}

void LocalCache::Release(const std::string& s3_key) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = cache_.find(s3_key);
    if (it != cache_.end()) {
        auto& mapped = it->second;
        if (mapped.addr) {
            munlock(mapped.addr, mapped.size);
            munmap(mapped.addr, mapped.size);
        }
        if (mapped.fd != -1) {
            close(mapped.fd);
        }
        cache_.erase(it);
    }
}

} // namespace greengate
