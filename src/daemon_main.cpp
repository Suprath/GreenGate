#include "green_gate/query_daemon.hpp"
#include <iostream>
#include <cstdlib>
#include <thread>
#include <chrono>

int main() {
    // Disable output buffering to ensure logs are flushed immediately to stdout
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);

    const char* port_env = std::getenv("PORT");
    const char* cache_dir_env = std::getenv("CACHE_DIR");
    const char* s3_endpoint_env = std::getenv("S3_ENDPOINT");
    const char* s3_bucket_env = std::getenv("S3_BUCKET");

    int port = port_env ? std::stoi(port_env) : 8080;
    std::string cache_dir = cache_dir_env ? cache_dir_env : "/tmp/greengate_cache";
    std::string s3_endpoint = s3_endpoint_env ? s3_endpoint_env : "http://localhost:9000";
    std::string s3_bucket = s3_bucket_env ? s3_bucket_env : "greengate";

    std::cout << "Starting GreenGate Query Daemon...\n";
    std::cout << "Configuration:\n";
    std::cout << " - Port:        " << port << "\n";
    std::cout << " - Cache Dir:  " << cache_dir << "\n";
    std::cout << " - S3 Endpoint: " << s3_endpoint << "\n";
    std::cout << " - S3 Bucket:   " << s3_bucket << "\n";

    try {
        greengate::QueryDaemon daemon(port, cache_dir, s3_endpoint, s3_bucket);
        daemon.Start();

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ Daemon crashed with error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
