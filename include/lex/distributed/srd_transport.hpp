#pragma once
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <condition_variable>
#include <atomic>

namespace greengate {

struct FatSignature {
    uint64_t kim;
    uint64_t xor_sum;

    bool operator==(const FatSignature& o) const {
        return kim == o.kim && xor_sum == o.xor_sum;
    }
};

} // namespace greengate

namespace std {
template <>
struct hash<greengate::FatSignature> {
    size_t operator()(const greengate::FatSignature& s) const {
        return s.kim ^ s.xor_sum;
    }
};
} // namespace std

namespace greengate {

struct ShuffleEntry {
    FatSignature sig;
    uint32_t row_id;
};

struct SrdHeader {
    uint32_t sender_node_id;
    uint32_t msg_id;
    uint32_t packet_seq;
    uint32_t total_packets;
    uint32_t payload_len;
};

class SrdTransport {
public:
    SrdTransport(int node_id, const std::string& host, int port);
    ~SrdTransport();

    void Start();
    void Stop();

    // Sends shuffle entries to the target address, fragmenting and shuffling packets to simulate SRD
    void Send(const std::string& target_host, int target_port, const std::vector<ShuffleEntry>& entries);

    // Non-blocking retrieval of fully reassembled shuffle batches
    std::vector<std::vector<ShuffleEntry>> ReceiveAll();

    int GetPort() const { return port_; }
    int GetNodeId() const { return node_id_; }

private:
    void ReceiverLoop();

    int node_id_;
    std::string host_;
    int port_;
    int socket_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread receiver_thread_;

    std::mutex mutex_;
    struct MessageReconstruction {
        uint32_t total_packets = 0;
        uint32_t packets_received = 0;
        std::vector<std::vector<uint8_t>> packets;
    };
    std::unordered_map<uint64_t, MessageReconstruction> recon_buffers_;
    std::vector<std::vector<ShuffleEntry>> completed_messages_;

    std::atomic<uint32_t> next_msg_id_{1};
    uint64_t total_bytes_sent_ = 0;
    uint64_t total_bytes_received_ = 0;

public:
    uint64_t GetTotalBytesSent() const { return total_bytes_sent_; }
    uint64_t GetTotalBytesReceived() const { return total_bytes_received_; }
    void ResetStats() { total_bytes_sent_ = 0; total_bytes_received_ = 0; }
};

} // namespace greengate
