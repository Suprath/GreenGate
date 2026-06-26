#include "green_gate/s3_writer.hpp"
#include "green_gate/agb_format.hpp"
#include "apex/engine.hpp"
#include <curl/curl.h>
#include <iostream>
#include <vector>
#include <cstring>
#include <algorithm>

namespace greengate {

// Global curl initialization helper
struct CurlGlobalInit {
    CurlGlobalInit() { curl_global_init(CURL_GLOBAL_ALL); }
    ~CurlGlobalInit() { curl_global_cleanup(); }
};
static CurlGlobalInit g_curl_init;

S3Writer::S3Writer(const std::string& endpoint, const std::string& bucket,
                   const std::string& access_key, const std::string& secret_key)
    : endpoint_(endpoint), bucket_(bucket), access_key_(access_key), secret_key_(secret_key) {
    worker_ = std::thread(&S3Writer::WorkerLoop, this);
}

S3Writer::~S3Writer() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

void S3Writer::WriteAsync(const std::string& s3_key, 
                          const std::shared_ptr<arrow::RecordBatch>& batch) {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push({s3_key, batch});
        active_tasks_++;
    }
    cv_.notify_one();
}

void S3Writer::Flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    flush_cv_.wait(lock, [this]() { return active_tasks_ == 0 && queue_.empty(); });
}

struct ReadBuffer {
    const uint8_t* data;
    size_t size;
    size_t pos;
};

static size_t ReadCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
    auto* ubuf = static_cast<ReadBuffer*>(userdata);
    size_t total_size = size * nitems;
    if (ubuf->pos >= ubuf->size) return 0;
    size_t to_copy = std::min(total_size, ubuf->size - ubuf->pos);
    std::memcpy(buffer, ubuf->data + ubuf->pos, to_copy);
    ubuf->pos += to_copy;
    return to_copy;
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

bool S3Writer::UploadToS3(const std::string& s3_key, const uint8_t* data, size_t size) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    std::string url = GetS3Url(endpoint_, bucket_, s3_key);

    ReadBuffer ubuf{data, size, 0};

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)size);
    curl_easy_setopt(curl, CURLOPT_READDATA, &ubuf);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, ReadCallback);
    
    // Local dev: skip SSL checks if testing with local HTTPS MinIO
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "User-Agent: GreenGate-S3Writer");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        std::cerr << "❌ S3 upload failed for " << s3_key << ": " << curl_easy_strerror(res) << "\n";
        return false;
    }

    if (response_code < 200 || response_code >= 300) {
        std::cerr << "❌ S3 upload failed for " << s3_key << " with HTTP response code " << response_code << "\n";
        return false;
    }

    return true;
}

void S3Writer::WorkerLoop() {
    apex::ApexEngine temp_engine;
    ArrowConnector connector(temp_engine);

    while (true) {
        WriteRequest req;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) {
                break;
            }
            req = std::move(queue_.front());
            queue_.pop();
        }

        auto batch = req.batch;
        uint64_t num_records = batch->num_rows();
        uint64_t num_fields = batch->num_columns();
        uint64_t num_blocks = (num_records + 63) / 64;

        size_t schema_meta_size = num_fields * sizeof(AgbField);
        size_t data_size = num_blocks * num_fields * 64 * sizeof(uint64_t);
        size_t total_file_size = sizeof(AgbHeader) + schema_meta_size + data_size;

        std::vector<uint8_t> buffer(total_file_size);
        
        // Populate header
        AgbHeader* header = reinterpret_cast<AgbHeader*>(buffer.data());
        std::memcpy(header->magic, AGB_MAGIC, 4);
        header->num_records = num_records;
        header->num_fields = num_fields;
        header->num_blocks = num_blocks;

        // Populate fields metadata
        AgbField* fields = reinterpret_cast<AgbField*>(buffer.data() + sizeof(AgbHeader));
        auto schema = batch->schema();
        for (uint64_t f = 0; f < num_fields; ++f) {
            auto field = schema->field(f);
            std::strncpy(fields[f].name, field->name().c_str(), sizeof(fields[f].name) - 1);
            fields[f].name[sizeof(fields[f].name) - 1] = '\0';
            
            uint32_t type_id = 0; // default UINT64
            switch (field->type()->id()) {
                case arrow::Type::INT8:
                case arrow::Type::INT16:
                case arrow::Type::INT32:   type_id = 3; break; // INT32
                case arrow::Type::INT64:   type_id = 1; break; // INT64
                case arrow::Type::UINT8:
                case arrow::Type::UINT16:
                case arrow::Type::UINT32:  type_id = 2; break; // UINT32
                case arrow::Type::UINT64:  type_id = 0; break; // UINT64
                case arrow::Type::BOOL:    type_id = 2; break; // UINT32
                default: type_id = 0; break;
            }
            fields[f].type_id = type_id;
        }

        // Transpose directly into the payload region of the buffer
        uint64_t* bit_planes_start = reinterpret_cast<uint64_t*>(buffer.data() + sizeof(AgbHeader) + schema_meta_size);
        connector.TransposeBatch(batch, bit_planes_start);

        // Upload to S3
        UploadToS3(req.s3_key, buffer.data(), total_file_size);

        {
            std::unique_lock<std::mutex> lock(mutex_);
            active_tasks_--;
            if (active_tasks_ == 0 && queue_.empty()) {
                flush_cv_.notify_all();
            }
        }
    }
}

} // namespace greengate
