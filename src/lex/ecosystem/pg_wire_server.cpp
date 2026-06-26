#include "lex/ecosystem/pg_wire_server.hpp"
#include "lex/distributed/global_metadata.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <unordered_set>
#include <arpa/inet.h>

namespace greengate {

// Helper functions for writing PG protocol elements
static void WriteInt32(std::vector<char>& buf, int32_t val) {
    uint32_t net_val = htonl(val);
    const char* ptr = reinterpret_cast<const char*>(&net_val);
    buf.insert(buf.end(), ptr, ptr + 4);
}

static void WriteInt16(std::vector<char>& buf, int16_t val) {
    uint16_t net_val = htons(val);
    const char* ptr = reinterpret_cast<const char*>(&net_val);
    buf.insert(buf.end(), ptr, ptr + 2);
}

static void WriteString(std::vector<char>& buf, const std::string& str) {
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back('\0');
}

static std::string Trim(const std::string& s) {
    size_t first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = s.find_last_not_of(" \t\r\n");
    return s.substr(first, (last - first + 1));
}

PgWireServer::PgWireServer(int port, std::shared_ptr<DistributedCoordinator> coordinator)
    : port_(port), coordinator_(coordinator) {}

PgWireServer::~PgWireServer() {
    Stop();
}

void PgWireServer::Start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        throw std::runtime_error("PG Server: Failed to create socket");
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        close(server_fd_);
        throw std::runtime_error("PG Server: Failed to bind port " + std::to_string(port_));
    }

    if (listen(server_fd_, 10) == -1) {
        close(server_fd_);
        throw std::runtime_error("PG Server: Failed to listen");
    }

    running_ = true;
    accept_thread_ = std::thread(&PgWireServer::AcceptLoop, this);
    std::cout << "🐘 PgWireServer listening on port " << port_ << std::endl;
}

void PgWireServer::Stop() {
    if (running_.exchange(false)) {
        if (server_fd_ != -1) {
            shutdown(server_fd_, SHUT_RDWR);
            close(server_fd_);
            server_fd_ = -1;
        }
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
        std::cout << "PgWireServer stopped." << std::endl;
    }
}

void PgWireServer::AcceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &len);
        if (client_fd == -1) {
            if (!running_) break;
            continue;
        }

        std::thread(&PgWireServer::HandleClient, this, client_fd).detach();
    }
}

void PgWireServer::HandleClient(int client_fd) {
    // 1. Handshake Phase
    char startup_len_buf[4];
    if (recv(client_fd, startup_len_buf, 4, 0) != 4) {
        close(client_fd);
        return;
    }
    
    int32_t len = ntohl(*reinterpret_cast<int32_t*>(startup_len_buf));
    std::vector<char> startup_payload(len - 4);
    if (recv(client_fd, startup_payload.data(), len - 4, 0) != len - 4) {
        close(client_fd);
        return;
    }

    int32_t proto = ntohl(*reinterpret_cast<int32_t*>(startup_payload.data()));
    if (proto == 80877103) { // SSLRequest
        // Respond with 'N' (no SSL support)
        char n = 'N';
        send(client_fd, &n, 1, 0);
        
        // Read actual startup message
        if (recv(client_fd, startup_len_buf, 4, 0) != 4) {
            close(client_fd);
            return;
        }
        len = ntohl(*reinterpret_cast<int32_t*>(startup_len_buf));
        startup_payload.resize(len - 4);
        if (recv(client_fd, startup_payload.data(), len - 4, 0) != len - 4) {
            close(client_fd);
            return;
        }
    }

    // Auth OK
    std::vector<char> response;
    response.push_back('R');
    WriteInt32(response, 8);
    WriteInt32(response, 0); // Success

    // Parameter Status
    auto add_param = [&](const std::string& key, const std::string& val) {
        response.push_back('S');
        WriteInt32(response, 4 + key.size() + 1 + val.size() + 1);
        WriteString(response, key);
        WriteString(response, val);
    };
    add_param("server_version", "15.0");
    add_param("client_encoding", "UTF8");
    add_param("DateStyle", "ISO");

    // Ready for Query
    response.push_back('Z');
    WriteInt32(response, 5);
    response.push_back('I'); // Idle

    send(client_fd, response.data(), response.size(), 0);

    // 2. Command Phase
    while (running_) {
        char type_byte;
        ssize_t bytes_recv = recv(client_fd, &type_byte, 1, 0);
        if (bytes_recv <= 0) break;

        char len_buf[4];
        if (recv(client_fd, len_buf, 4, 0) != 4) break;
        int32_t msg_len = ntohl(*reinterpret_cast<int32_t*>(len_buf));

        std::vector<char> msg_payload(msg_len - 4);
        if (recv(client_fd, msg_payload.data(), msg_len - 4, 0) != msg_len - 4) break;

        if (type_byte == 'X') { // Terminate
            break;
        }

        if (type_byte == 'Q') { // Query
            std::string query_str(msg_payload.data());
            while (!query_str.empty() && (query_str.back() == '\r' || query_str.back() == '\n' || query_str.back() == ';' || query_str.back() == ' ')) {
                query_str.pop_back();
            }

            std::cout << "[PgWireServer] Received SQL: " << query_str << std::endl;

            // Simple SQL parsing
            std::string q_lower = query_str;
            std::transform(q_lower.begin(), q_lower.end(), q_lower.begin(), ::tolower);

            size_t select_pos = q_lower.find("select");
            size_t from_pos = q_lower.find("from");

            if (select_pos == std::string::npos || from_pos == std::string::npos) {
                // Send Error Response
                std::vector<char> err;
                err.push_back('E');
                std::string err_msg = "SFATAL\0C42601\0MSyntax error\0";
                WriteInt32(err, 4 + err_msg.size() + 1);
                err.insert(err.end(), err_msg.begin(), err_msg.end());
                err.push_back('\0');
                send(client_fd, err.data(), err.size(), 0);
                continue;
            }

            std::string proj_str = Trim(query_str.substr(select_pos + 6, from_pos - (select_pos + 6)));
            size_t where_pos = q_lower.find("where");
            std::string table_str;
            std::string where_str;

            if (where_pos != std::string::npos) {
                table_str = Trim(query_str.substr(from_pos + 4, where_pos - (from_pos + 4)));
                where_str = Trim(query_str.substr(where_pos + 5));
            } else {
                table_str = Trim(query_str.substr(from_pos + 4));
            }

            // Parse projections
            std::vector<std::string> proj_cols;
            std::vector<AggregationType> proj_aggs;
            std::stringstream ss(proj_str);
            std::string proj_item;
            while (std::getline(ss, proj_item, ',')) {
                proj_item = Trim(proj_item);
                std::string item_lower = proj_item;
                std::transform(item_lower.begin(), item_lower.end(), item_lower.begin(), ::tolower);
                
                if (item_lower == "count(*)" || item_lower == "count(1)") {
                    proj_aggs.push_back(AggregationType::COUNT);
                    proj_cols.push_back("");
                } else if (item_lower.rfind("sum(", 0) == 0) {
                    proj_aggs.push_back(AggregationType::SUM);
                    proj_cols.push_back(proj_item.substr(4, proj_item.size() - 5));
                } else if (item_lower.rfind("avg(", 0) == 0) {
                    proj_aggs.push_back(AggregationType::AVG);
                    proj_cols.push_back(proj_item.substr(4, proj_item.size() - 5));
                } else if (item_lower.rfind("min(", 0) == 0) {
                    proj_aggs.push_back(AggregationType::MIN);
                    proj_cols.push_back(proj_item.substr(4, proj_item.size() - 5));
                } else if (item_lower.rfind("max(", 0) == 0) {
                    proj_aggs.push_back(AggregationType::MAX);
                    proj_cols.push_back(proj_item.substr(4, proj_item.size() - 5));
                } else if (item_lower.rfind("median(", 0) == 0) {
                    proj_aggs.push_back(AggregationType::MEDIAN);
                    proj_cols.push_back(proj_item.substr(7, proj_item.size() - 8));
                } else if (item_lower.rfind("distinct ", 0) == 0) {
                    proj_aggs.push_back(AggregationType::DISTINCT);
                    proj_cols.push_back(proj_item.substr(9));
                } else {
                    proj_aggs.push_back(AggregationType::COUNT); // Fallback
                    proj_cols.push_back(proj_item);
                }
            }

            // Parse WHERE predicates
            std::vector<Predicate> predicates;
            if (!where_str.empty()) {
                std::vector<std::string> clauses;
                std::string s = where_str;
                std::string s_lower = s;
                std::transform(s_lower.begin(), s_lower.end(), s_lower.begin(), ::tolower);
                
                size_t pos = 0;
                while (true) {
                    size_t and_pos = s_lower.find("and", pos);
                    if (and_pos == std::string::npos) {
                        clauses.push_back(Trim(s.substr(pos)));
                        break;
                    }
                    
                    bool before_ok = (and_pos == 0 || isspace(s_lower[and_pos - 1]) || s_lower[and_pos - 1] == ')');
                    bool after_ok = (and_pos + 3 >= s_lower.size() || isspace(s_lower[and_pos + 3]) || s_lower[and_pos + 3] == '(');
                    
                    if (before_ok && after_ok) {
                        clauses.push_back(Trim(s.substr(pos, and_pos - pos)));
                        pos = and_pos + 3;
                    } else {
                        pos = and_pos + 3;
                    }
                }

                for (auto& clause : clauses) {
                    clause = Trim(clause);
                    if (clause.empty()) continue;

                    size_t op_pos = std::string::npos;
                    PredicateOp op = PredicateOp::EQ;
                    size_t op_len = 1;
                    if ((op_pos = clause.find('=')) != std::string::npos) {
                        op = PredicateOp::EQ;
                    } else if ((op_pos = clause.find('>')) != std::string::npos) {
                        op = PredicateOp::GT;
                    } else if ((op_pos = clause.find('<')) != std::string::npos) {
                        op = PredicateOp::LT;
                    }

                    if (op_pos != std::string::npos) {
                        std::string col = Trim(clause.substr(0, op_pos));
                        std::string val_str = Trim(clause.substr(op_pos + op_len));
                        
                        Predicate p;
                        p.column_name = col;
                        p.op = op;
                        if (!val_str.empty() && (val_str.front() == '\'' || val_str.front() == '"')) {
                            p.is_string = true;
                            p.value = val_str.substr(1, val_str.size() - 2);
                        } else {
                            p.is_string = false;
                            p.numeric_value = std::stoull(val_str);
                        }
                        predicates.push_back(p);
                    }
                }
            }

            // Execute Query
            QueryResult res;
            
            // Check if it's a join query
            std::string table_lower = table_str;
            std::transform(table_lower.begin(), table_lower.end(), table_lower.begin(), ::tolower);
            
            if (table_lower.find("join") != std::string::npos) {
                // Parse L JOIN R ON L.col = R.col
                size_t join_pos = table_lower.find("join");
                size_t on_pos = table_lower.find("on");
                
                std::string left_t = Trim(table_str.substr(0, join_pos));
                std::string right_t = Trim(table_str.substr(join_pos + 4, on_pos - (join_pos + 4)));
                std::string on_clause = Trim(table_str.substr(on_pos + 2));
                
                size_t eq_pos = on_clause.find('=');
                std::string left_join_col = Trim(on_clause.substr(0, eq_pos));
                std::string right_join_col = Trim(on_clause.substr(eq_pos + 1));
                
                // Strip table prefixes if any
                if (left_join_col.find('.') != std::string::npos) {
                    left_join_col = left_join_col.substr(left_join_col.find('.') + 1);
                }
                if (right_join_col.find('.') != std::string::npos) {
                    right_join_col = right_join_col.substr(right_join_col.find('.') + 1);
                }

                // Partition predicates between Left and Right tables
                std::vector<Predicate> left_preds;
                std::vector<Predicate> right_preds;
                for (auto& p : predicates) {
                    if (p.column_name.rfind("L.", 0) == 0) {
                        Predicate cp = p;
                        cp.column_name = p.column_name.substr(2);
                        left_preds.push_back(cp);
                    } else if (p.column_name.rfind("R.", 0) == 0) {
                        Predicate cp = p;
                        cp.column_name = p.column_name.substr(2);
                        right_preds.push_back(cp);
                    } else {
                        left_preds.push_back(p);
                    }
                }

                AggregationType target_agg = proj_aggs[0];
                std::string agg_col = proj_cols[0];
                if (agg_col.rfind("L.", 0) == 0) {
                    agg_col = agg_col.substr(2);
                } else if (agg_col.rfind("R.", 0) == 0) {
                    agg_col = agg_col.substr(2);
                }

                res = coordinator_->ExecuteJoinQueryExtended(left_t, right_t, left_join_col, right_join_col, left_preds, right_preds, agg_col, target_agg);
            } else {
                // Single table query
                auto state = GlobalMetadata::Instance().GetGlobalState(table_str);
                if (state) {
                    AggregationType target_agg = proj_aggs[0];
                    std::string agg_col = proj_cols[0];
                    
                    if (!state->persisted_groups.empty()) {
                        QueryRunner runner = QueryPlanner::Plan(predicates, *state->persisted_groups[0], agg_col, target_agg);
                        res = runner.ExecuteQueryResult(*state);
                    } else if (state->active_memtable && state->active_memtable->GetRowCount() > 0) {
                        AdaptiveIngester ingester;
                        auto batches = state->active_memtable->GetBatches();
                        RowGroup dummy_rg = ingester.Ingest(batches[0]);
                        QueryRunner runner = QueryPlanner::Plan(predicates, dummy_rg, agg_col, target_agg);
                        res = runner.ExecuteQueryResult(*state);
                    }
                }
            }

            // Build result protocol response
            std::vector<char> q_resp;

            // 1. RowDescription ('T')
            q_resp.push_back('T');
            std::vector<char> desc_payload;
            WriteInt16(desc_payload, static_cast<int16_t>(proj_aggs.size()));
            
            for (size_t i = 0; i < proj_aggs.size(); ++i) {
                WriteString(desc_payload, proj_str); // Field name
                WriteInt32(desc_payload, 0); // Table OID
                WriteInt16(desc_payload, 0); // Column Index
                
                // Type OID
                if (proj_aggs[i] == AggregationType::COUNT) {
                    WriteInt32(desc_payload, 20); // INT8 OID
                    WriteInt16(desc_payload, 8);  // Size
                } else if (proj_aggs[i] == AggregationType::AVG) {
                    WriteInt32(desc_payload, 701); // FLOAT8 OID
                    WriteInt16(desc_payload, 8);
                } else if (proj_aggs[i] == AggregationType::DISTINCT && !res.distinct_strings.empty()) {
                    WriteInt32(desc_payload, 1043); // VARCHAR OID
                    WriteInt16(desc_payload, -1);
                } else {
                    WriteInt32(desc_payload, 20); // INT8 OID
                    WriteInt16(desc_payload, 8);
                }
                
                WriteInt32(desc_payload, -1); // Type Modifier
                WriteInt16(desc_payload, 0);  // Format (Text)
            }
            WriteInt32(q_resp, 4 + desc_payload.size());
            q_resp.insert(q_resp.end(), desc_payload.begin(), desc_payload.end());

            // 2. DataRow ('D')
            q_resp.push_back('D');
            std::vector<char> row_payload;
            WriteInt16(row_payload, static_cast<int16_t>(proj_aggs.size()));
            
            for (size_t i = 0; i < proj_aggs.size(); ++i) {
                std::string val_str;
                switch (proj_aggs[i]) {
                    case AggregationType::COUNT:
                        val_str = std::to_string(res.count);
                        break;
                    case AggregationType::SUM:
                        val_str = std::to_string(res.sum);
                        break;
                    case AggregationType::AVG:
                        val_str = std::to_string(res.avg);
                        break;
                    case AggregationType::MIN:
                        val_str = std::to_string(res.min_val);
                        break;
                    case AggregationType::MAX:
                        val_str = std::to_string(res.max_val);
                        break;
                    case AggregationType::MEDIAN:
                        val_str = std::to_string(res.median_val);
                        break;
                    case AggregationType::DISTINCT:
                        val_str = std::to_string(res.distinct_count);
                        break;
                }
                WriteInt32(row_payload, static_cast<int32_t>(val_str.size()));
                row_payload.insert(row_payload.end(), val_str.begin(), val_str.end());
            }
            WriteInt32(q_resp, 4 + row_payload.size());
            q_resp.insert(q_resp.end(), row_payload.begin(), row_payload.end());

            // 3. CommandComplete ('C')
            q_resp.push_back('C');
            std::vector<char> cc_payload;
            WriteString(cc_payload, "SELECT 1");
            WriteInt32(q_resp, 4 + cc_payload.size());
            q_resp.insert(q_resp.end(), cc_payload.begin(), cc_payload.end());

            // 4. ReadyForQuery ('Z')
            q_resp.push_back('Z');
            WriteInt32(q_resp, 5);
            q_resp.push_back('I');

            send(client_fd, q_resp.data(), q_resp.size(), 0);
        }
    }

    close(client_fd);
}

} // namespace greengate
