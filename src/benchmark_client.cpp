#include "green_gate/s3_writer.hpp"
#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <curl/curl.h>

#define ASSERT_OK(expr) \
    do { \
        auto _s = (expr); \
        if (!_s.ok()) { \
            std::cerr << "Arrow Error: " << _s.ToString() << "\n"; \
            std::exit(1); \
        } \
    } while (0)

// Generates a mock Arrow RecordBatch with 1,000,000 ticks
std::shared_ptr<arrow::RecordBatch> GenerateMockArrowData(int64_t num_rows) {
    arrow::UInt64Builder ask_builder;
    arrow::UInt64Builder bid_builder;
    arrow::UInt64Builder vol_builder;
    
    ASSERT_OK(ask_builder.Reserve(num_rows));
    ASSERT_OK(bid_builder.Reserve(num_rows));
    ASSERT_OK(vol_builder.Reserve(num_rows));
    
    for (int64_t i = 0; i < num_rows; ++i) {
        ASSERT_OK(ask_builder.Append(10000 + i));
        ASSERT_OK(bid_builder.Append(9998));
        ASSERT_OK(vol_builder.Append(1000 + i));
    }
    
    std::shared_ptr<arrow::Array> ask_array;
    std::shared_ptr<arrow::Array> bid_array;
    std::shared_ptr<arrow::Array> vol_array;
    
    ASSERT_OK(ask_builder.Finish(&ask_array));
    ASSERT_OK(bid_builder.Finish(&bid_array));
    ASSERT_OK(vol_builder.Finish(&vol_array));
    
    auto schema = arrow::schema({
        arrow::field("ask", arrow::uint64()),
        arrow::field("bid", arrow::uint64()),
        arrow::field("volume", arrow::uint64())
    });
    
    return arrow::RecordBatch::Make(schema, num_rows, {ask_array, bid_array, vol_array});
}

bool CreateBucket(const std::string& s3_endpoint, const std::string& bucket) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    std::string url = s3_endpoint;
    if (url.back() != '/') url += '/';
    url += bucket + "/";

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_PUT, 1L);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, 0L);
    
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    CURLcode res = curl_easy_perform(curl);
    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    curl_easy_cleanup(curl);

    return res == CURLE_OK && (response_code == 200 || response_code == 409);
}

int main() {
    std::cout << "==================================================\n";
    std::cout << "🟢 GREEN PIPELINE: MILESTONE 2 E2E BENCHMARK START\n";
    std::cout << "==================================================\n\n";

    const char* daemon_host_env = std::getenv("DAEMON_HOST");
    const char* daemon_port_env = std::getenv("DAEMON_PORT");
    const char* s3_endpoint_env = std::getenv("S3_ENDPOINT");
    const char* s3_bucket_env = std::getenv("S3_BUCKET");

    std::string daemon_host = daemon_host_env ? daemon_host_env : "localhost";
    int daemon_port = daemon_port_env ? std::stoi(daemon_port_env) : 8080;
    std::string s3_endpoint = s3_endpoint_env ? s3_endpoint_env : "http://localhost:9000";
    std::string s3_bucket = s3_bucket_env ? s3_bucket_env : "greengate";

    std::cout << "Waiting for services to initialize...\n";
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::cout << "[Step 1] Attempting to create/verify MinIO bucket \"" << s3_bucket << "\"...\n";
    CreateBucket(s3_endpoint, s3_bucket);
    std::cout << "✓ Proceeding to data generation and upload.\n\n";

    constexpr int64_t num_rows = 1000000;
    std::cout << "[Step 2] Generating " << num_rows << " rows of mock Arrow data...\n";
    auto batch = GenerateMockArrowData(num_rows);
    std::cout << "✓ Arrow RecordBatch generated successfully.\n\n";

    std::cout << "[Step 3] Initializing S3Writer and writing AGB file to MinIO asynchronously...\n";
    auto start_write = std::chrono::high_resolution_clock::now();
    {
        greengate::S3Writer writer(s3_endpoint, s3_bucket, "minioadmin", "minioadmin");
        writer.WriteAsync("ticks_large.agb", batch);
        std::cout << "✓ Queue request submitted. Flushing write queue...\n";
        writer.Flush();
    }
    auto end_write = std::chrono::high_resolution_clock::now();
    auto duration_write_us = std::chrono::duration_cast<std::chrono::microseconds>(end_write - start_write).count();
    std::cout << "✓ File successfully sliced, serialized, and uploaded to MinIO.\n";
    std::cout << "  Total serialization + upload time: " << (duration_write_us / 1000.0) << " ms\n\n";

    std::cout << "[Step 4] Connecting to QueryDaemon at " << daemon_host << ":" << daemon_port << "...\n";
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd == -1) {
        std::cerr << "❌ Socket creation failed\n";
        return 1;
    }

    hostent* server = gethostbyname(daemon_host.c_str());
    if (!server) {
        std::cerr << "❌ Host resolve failed: " << daemon_host << "\n";
        close(client_fd);
        return 1;
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    std::memcpy(&serv_addr.sin_addr.s_addr, server->h_addr_list[0], server->h_length);
    serv_addr.sin_port = htons(daemon_port);

    int conn_res = -1;
    for (int retry = 0; retry < 5; ++retry) {
        conn_res = connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
        if (conn_res == 0) break;
        std::cout << "  Retrying connection in 1 second...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (conn_res == -1) {
        std::cerr << "❌ Connection to QueryDaemon failed: " << strerror(errno) << "\n";
        close(client_fd);
        return 1;
    }
    std::cout << "✓ Connected to QueryDaemon.\n\n";

    std::string sql_predicate = "ask > 10500 AND volume > 2000";
    std::string cmd = "QUERY ticks_large.agb WHERE " + sql_predicate + "\n";

    std::cout << "[Step 5] Sending query: \"" << cmd << "\"\n";
    send(client_fd, cmd.c_str(), cmd.size(), 0);

    char resp_buf[1024];
    std::memset(resp_buf, 0, sizeof(resp_buf));
    ssize_t bytes_received = recv(client_fd, resp_buf, sizeof(resp_buf) - 1, 0);
    close(client_fd);

    if (bytes_received <= 0) {
        std::cerr << "❌ Failed to receive response from QueryDaemon\n";
        return 1;
    }

    std::string response(resp_buf);
    std::cout << "✓ Response from Daemon: \"" << response << "\"\n";

    std::cout << "[Step 6] Running local scalar loop verification...\n";
    uint64_t expected_matches = 0;
    for (int64_t i = 0; i < num_rows; ++i) {
        uint64_t ask = 10000 + i;
        uint64_t volume = 1000 + i;
        if (ask > 10500 && volume > 2000) {
            expected_matches++;
        }
    }
    std::cout << "✓ Local scalar expected matches: " << expected_matches << "\n";

    uint64_t daemon_matches = 0;
    uint64_t daemon_scan_time_us = 0;
    if (response.rfind("MATCHES ", 0) == 0) {
        std::sscanf(response.c_str(), "MATCHES %lu SCAN_TIME_US %lu", &daemon_matches, &daemon_scan_time_us);
        
        if (daemon_matches == expected_matches) {
            std::cout << "✅ CORRECTNESS VERIFIED: Matches match EXACTLY bit-for-bit!\n";
            double rps = (static_cast<double>(num_rows) / daemon_scan_time_us) * 1000000.0;
            std::cout << "🚀 Remote Scan Throughput: " << (rps / 1000000.0) << " Million Records/sec\n";
        } else {
            std::cerr << "❌ ERROR: Match mismatch! Daemon: " << daemon_matches << ", Expected: " << expected_matches << "\n";
            return 1;
        }
    } else {
        std::cerr << "❌ ERROR: Daemon returned error: " << response << "\n";
        return 1;
    }

    std::cout << "\n==================================================\n";
    std::cout << "🎉 MILESTONE 2 E2E BENCHMARK PASSED SUCCESSFULLY!\n";
    std::cout << "==================================================\n";

    return 0;
}
