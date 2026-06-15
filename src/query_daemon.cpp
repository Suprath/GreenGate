#include "green_gate/query_daemon.hpp"
#include "green_gate/agb_format.hpp"
#include "green_gate/sql_parser.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <chrono>
#include <cstring>
#include <vector>

namespace greengate {

QueryDaemon::QueryDaemon(int port, const std::string& cache_dir, 
                         const std::string& s3_endpoint, const std::string& bucket)
    : port_(port), cache_(cache_dir, s3_endpoint, bucket) {}

QueryDaemon::~QueryDaemon() {
    Stop();
}

void QueryDaemon::Start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        throw std::runtime_error("Failed to create TCP socket: " + std::string(strerror(errno)));
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        close(server_fd_);
        throw std::runtime_error("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) == -1) {
        close(server_fd_);
        throw std::runtime_error("Failed to bind TCP socket: " + std::string(strerror(errno)));
    }

    if (listen(server_fd_, 10) == -1) {
        close(server_fd_);
        throw std::runtime_error("Failed to listen on TCP socket: " + std::string(strerror(errno)));
    }

    running_ = true;
    accept_thread_ = std::thread(&QueryDaemon::AcceptLoop, this);
    std::cout << "🚀 QueryDaemon listening on port " << port_ << "\n";
}

void QueryDaemon::Stop() {
    if (running_.exchange(false)) {
        if (server_fd_ != -1) {
            shutdown(server_fd_, SHUT_RDWR);
            close(server_fd_);
            server_fd_ = -1;
        }
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        std::cout << "QueryDaemon stopped.\n";
    }
}

void QueryDaemon::AcceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd == -1) {
            if (!running_) break;
            std::cerr << "❌ accept failed: " << strerror(errno) << "\n";
            continue;
        }

        std::thread([this, client_fd]() {
            HandleClient(client_fd);
        }).detach();
    }
}

void QueryDaemon::HandleClient(int client_fd) {
    char buffer[4096];
    std::string request;

    while (true) {
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_read <= 0) {
            break;
        }
        buffer[bytes_read] = '\0';
        request += buffer;
        if (request.find('\n') != std::string::npos) {
            break;
        }
    }

    while (!request.empty() && (request.back() == '\r' || request.back() == '\n' || request.back() == ' ')) {
        request.pop_back();
    }

    std::ostringstream response;
    
    if (request.rfind("QUERY ", 0) != 0) {
        response << "ERROR Invalid command format. Expected: QUERY <s3_key> WHERE <predicate>\n";
    } else {
        size_t where_pos = request.find(" WHERE ");
        if (where_pos == std::string::npos) {
            response << "ERROR Missing WHERE clause\n";
        } else {
            std::string s3_key = request.substr(6, where_pos - 6);
            std::string predicate = request.substr(where_pos + 7);

            try {
                MappedFile file = cache_.GetOrDownload(s3_key);
                
                AgbHeader* header = reinterpret_cast<AgbHeader*>(file.addr);
                if (std::memcmp(header->magic, AGB_MAGIC, 4) != 0) {
                    throw std::runtime_error("Invalid AGB magic bytes");
                }

                if (!engine_.get_registry().has_schema(s3_key)) {
                    std::vector<apex::core::FieldDescriptor> fields;
                    AgbField* agb_fields = reinterpret_cast<AgbField*>((char*)file.addr + sizeof(AgbHeader));
                    size_t offset = 0;
                    for (uint64_t i = 0; i < header->num_fields; ++i) {
                        apex::core::DataType type = static_cast<apex::core::DataType>(agb_fields[i].type_id);
                        fields.push_back({std::string(agb_fields[i].name), static_cast<uint32_t>(offset), 64, type});
                        offset += sizeof(uint64_t);
                    }
                    engine_.register_schema(s3_key, fields, offset);
                    std::cout << "✓ Registered dynamic schema for " << s3_key << " with " << header->num_fields << " fields.\n";
                }

                SqlParser parser;
                apex::ir::Node* ast_root = parser.Parse(predicate);
                engine_.set_expression(s3_key, ast_root);

                uint64_t* bit_planes = reinterpret_cast<uint64_t*>(
                    (char*)file.addr + sizeof(AgbHeader) + header->num_fields * sizeof(AgbField)
                );

                auto start_time = std::chrono::high_resolution_clock::now();
                uint64_t matches = engine_.execute_native(s3_key, bit_planes, header->num_blocks);
                auto end_time = std::chrono::high_resolution_clock::now();

                auto scan_time_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
                response << "MATCHES " << matches << " SCAN_TIME_US " << scan_time_us << "\n";

            } catch (const std::exception& e) {
                response << "ERROR " << e.what() << "\n";
            }
        }
    }

    std::string resp_str = response.str();
    send(client_fd, resp_str.c_str(), resp_str.size(), 0);
    close(client_fd);
}

} // namespace greengate
