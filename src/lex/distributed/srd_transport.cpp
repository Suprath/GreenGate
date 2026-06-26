#include "lex/distributed/srd_transport.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <random>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <iostream>
#include <cerrno>

namespace greengate {

SrdTransport::SrdTransport(int node_id, const std::string& host, int port)
    : node_id_(node_id), host_(host), port_(port) {}

SrdTransport::~SrdTransport() {
    Stop();
}

void SrdTransport::Start() {
    socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd_ < 0) {
        throw std::runtime_error("Failed to create UDP socket: " + std::string(strerror(errno)));
    }

    int opt = 1;
    setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 50000; // 50ms receive timeout
    setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        throw std::runtime_error("Invalid IP address: " + host_);
    }

    if (bind(socket_fd_, reinterpret_cast<const struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error("Failed to bind socket on port " + std::to_string(port_) + ": " + std::string(strerror(errno)));
    }

    running_ = true;
    receiver_thread_ = std::thread(&SrdTransport::ReceiverLoop, this);
    std::cout << "[SrdTransport] Node " << node_id_ << " listening on " << host_ << ":" << port_ << std::endl;
}

void SrdTransport::Stop() {
    if (running_) {
        running_ = false;
        if (receiver_thread_.joinable()) {
            receiver_thread_.join();
        }
    }
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
}

void SrdTransport::Send(const std::string& target_host, int target_port, const std::vector<ShuffleEntry>& entries) {
    if (entries.empty()) return;
    if (socket_fd_ < 0) return;

    uint32_t msg_id = next_msg_id_++;
    size_t num_entries = entries.size();
    const size_t entries_per_packet = 5; // Use small size to force multi-packet fragmentation
    size_t total_packets = (num_entries + entries_per_packet - 1) / entries_per_packet;

    std::vector<std::vector<uint8_t>> packet_buffers(total_packets);
    for (size_t p = 0; p < total_packets; ++p) {
        size_t start_idx = p * entries_per_packet;
        size_t end_idx = std::min(start_idx + entries_per_packet, num_entries);
        size_t payload_len = (end_idx - start_idx) * sizeof(ShuffleEntry);

        packet_buffers[p].resize(sizeof(SrdHeader) + payload_len);
        SrdHeader* header = reinterpret_cast<SrdHeader*>(packet_buffers[p].data());
        header->sender_node_id = static_cast<uint32_t>(node_id_);
        header->msg_id = msg_id;
        header->packet_seq = static_cast<uint32_t>(p);
        header->total_packets = static_cast<uint32_t>(total_packets);
        header->payload_len = static_cast<uint32_t>(payload_len);

        std::memcpy(packet_buffers[p].data() + sizeof(SrdHeader), &entries[start_idx], payload_len);
    }

    struct sockaddr_in dest_addr;
    std::memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(target_port);
    if (inet_pton(AF_INET, target_host.c_str(), &dest_addr.sin_addr) <= 0) {
        return;
    }

    // Shuffle packets to simulate out-of-order (multi-path) arrival
    std::vector<size_t> packet_indices(total_packets);
    std::iota(packet_indices.begin(), packet_indices.end(), 0);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(packet_indices.begin(), packet_indices.end(), g);

    for (size_t p : packet_indices) {
        const auto& pkt = packet_buffers[p];
        ssize_t sent = sendto(socket_fd_, pkt.data(), pkt.size(), 0,
                              reinterpret_cast<const struct sockaddr*>(&dest_addr), sizeof(dest_addr));
        if (sent > 0) {
            total_bytes_sent_ += sent;
        }
        // Brief sleep to allow packet scheduling out-of-order
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
}

std::vector<std::vector<ShuffleEntry>> SrdTransport::ReceiveAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto msgs = std::move(completed_messages_);
    completed_messages_.clear();
    return msgs;
}

void SrdTransport::ReceiverLoop() {
    std::vector<uint8_t> recv_buf(1500);
    struct sockaddr_in src_addr;
    socklen_t addr_len = sizeof(src_addr);

    while (running_) {
        ssize_t received = recvfrom(socket_fd_, recv_buf.data(), recv_buf.size(), 0,
                                    reinterpret_cast<struct sockaddr*>(&src_addr), &addr_len);
        if (received < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            break;
        }

        if (received < static_cast<ssize_t>(sizeof(SrdHeader))) {
            continue;
        }

        total_bytes_received_ += received;

        const SrdHeader* header = reinterpret_cast<const SrdHeader*>(recv_buf.data());
        if (received < static_cast<ssize_t>(sizeof(SrdHeader) + header->payload_len)) {
            continue;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t recon_key = (static_cast<uint64_t>(header->sender_node_id) << 32) | header->msg_id;
        auto& msg = recon_buffers_[recon_key];
        if (msg.total_packets == 0) {
            msg.total_packets = header->total_packets;
            msg.packets.resize(header->total_packets);
        }

        if (header->packet_seq < msg.total_packets && msg.packets[header->packet_seq].empty()) {
            msg.packets[header->packet_seq].assign(recv_buf.data() + sizeof(SrdHeader),
                                                   recv_buf.data() + sizeof(SrdHeader) + header->payload_len);
            msg.packets_received++;

            if (msg.packets_received == msg.total_packets) {
                std::vector<ShuffleEntry> assembled_entries;
                for (const auto& pkt_payload : msg.packets) {
                    size_t num_entries = pkt_payload.size() / sizeof(ShuffleEntry);
                    const ShuffleEntry* entries_ptr = reinterpret_cast<const ShuffleEntry*>(pkt_payload.data());
                    assembled_entries.insert(assembled_entries.end(), entries_ptr, entries_ptr + num_entries);
                }
                completed_messages_.push_back(std::move(assembled_entries));
                recon_buffers_.erase(recon_key);
            }
        }
    }
}

} // namespace greengate
