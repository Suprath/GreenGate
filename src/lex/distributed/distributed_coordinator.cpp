#include "lex/distributed/distributed_coordinator.hpp"
#include "lex/distributed/global_metadata.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include <iostream>
#include <unordered_set>
#include <cassert>

namespace greengate {

// Local helpers for hashing
static uint64_t GenerateXorSum(std::string_view str) {
    uint64_t hash = 14695981039346656037ULL;
    for (char c : str) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

static uint64_t GenerateKimSignature(std::string_view str) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    if (str.empty()) return hash;
    if (str.size() == 1) {
        hash ^= static_cast<uint8_t>(str[0]);
        hash *= 0x100000001b3ULL;
        return hash;
    }
    for (size_t i = 0; i < str.size() - 1; ++i) {
        uint32_t bigram = (static_cast<uint32_t>(static_cast<uint8_t>(str[i])) << 8) | 
                           static_cast<uint32_t>(static_cast<uint8_t>(str[i+1]));
        hash ^= bigram;
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

static uint32_t PackRowId(uint32_t node_id, uint32_t batch_idx, uint32_t row_idx) {
    return (node_id << 24) | (batch_idx << 12) | row_idx;
}

static void UnpackRowId(uint32_t packed, uint32_t& node_id, uint32_t& batch_idx, uint32_t& row_idx) {
    node_id = packed >> 24;
    batch_idx = (packed >> 12) & 0xFFF;
    row_idx = packed & 0xFFF;
}

static std::vector<bool> GetMatchingRows(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<Predicate>& preds) {
    size_t num_rows = batch->num_rows();
    std::vector<bool> matches(num_rows, true);
    
    for (const auto& pred : preds) {
        auto col = batch->GetColumnByName(pred.column_name);
        if (!col) {
            std::fill(matches.begin(), matches.end(), false);
            break;
        }
        
        if (pred.is_string) {
            auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
            std::string clean_pat = pred.value;
            if (pred.op == PredicateOp::LIKE_PREFIX && !clean_pat.empty() && clean_pat.back() == '%') {
                clean_pat.pop_back();
            } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                if (!clean_pat.empty() && clean_pat.front() == '%') clean_pat.erase(0, 1);
                if (!clean_pat.empty() && clean_pat.back() == '%') clean_pat.pop_back();
            }
            
            for (size_t r = 0; r < num_rows; ++r) {
                if (!matches[r]) continue;
                if (str_arr->IsNull(r)) {
                    matches[r] = false;
                } else {
                    std::string_view val = str_arr->GetView(r);
                    if (pred.op == PredicateOp::EQ) {
                        matches[r] = (val == clean_pat);
                    } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                        matches[r] = (val.size() >= clean_pat.size() && val.compare(0, clean_pat.size(), clean_pat) == 0);
                    } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                        matches[r] = (val.find(clean_pat) != std::string_view::npos);
                    }
                }
            }
        } else {
            auto int_arr = std::static_pointer_cast<arrow::Int64Array>(col);
            int64_t target = static_cast<int64_t>(pred.numeric_value);
            for (size_t r = 0; r < num_rows; ++r) {
                if (!matches[r]) continue;
                if (int_arr->IsNull(r)) {
                    matches[r] = false;
                } else {
                    int64_t val = int_arr->Value(r);
                    if (pred.op == PredicateOp::EQ) matches[r] = (val == target);
                    else if (pred.op == PredicateOp::GT) matches[r] = (val > target);
                    else if (pred.op == PredicateOp::LT) matches[r] = (val < target);
                }
            }
        }
    }
    return matches;
}

// ==========================================
// ClusterNode Implementation
// ==========================================

ClusterNode::ClusterNode(int node_id, const std::string& host, int port, int num_nodes)
    : node_id_(node_id), num_nodes_(num_nodes), transport_(node_id, host, port) {
    // Assign default Availability Zone based on node_id
    availability_zone_ = "us-east-1-az" + std::to_string(node_id + 1);
}

ClusterNode::~ClusterNode() {
    Stop();
}

void ClusterNode::Start() {
    transport_.Start();
}

void ClusterNode::Stop() {
    transport_.Stop();
}

void ClusterNode::IngestLeftPartition(const std::shared_ptr<arrow::RecordBatch>& batch) {
    left_batches_.push_back(batch);
    AdaptiveIngester ingester;
    left_row_groups_.push_back(ingester.Ingest(batch));
}

void ClusterNode::IngestRightPartition(const std::shared_ptr<arrow::RecordBatch>& batch) {
    right_batches_.push_back(batch);
    AdaptiveIngester ingester;
    right_row_groups_.push_back(ingester.Ingest(batch));
}

void ClusterNode::PrepareJoinSignatures(const std::string& left_table, const std::string& right_table,
                                       const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                                       const std::string& left_join_col, const std::string& right_join_col) {
    (void)left_table;
    (void)right_table;
    local_left_signatures_.clear();
    local_right_signatures_.clear();

    // Process Left Partition Batches
    for (size_t b = 0; b < left_batches_.size(); ++b) {
        const auto& batch = left_batches_[b];
        std::vector<bool> matches = GetMatchingRows(batch, left_preds);
        auto col = batch->GetColumnByName(left_join_col);
        if (!col) continue;
        auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);

        for (size_t r = 0; r < static_cast<size_t>(batch->num_rows()); ++r) {
            if (matches[r] && !str_arr->IsNull(r)) {
                std::string_view val = str_arr->GetView(r);
                FatSignature sig{GenerateKimSignature(val), GenerateXorSum(val)};
                uint32_t packed_id = PackRowId(node_id_, static_cast<uint32_t>(b), static_cast<uint32_t>(r));
                local_left_signatures_.push_back({sig, packed_id});
            }
        }
    }

    // Process Right Partition Batches
    for (size_t b = 0; b < right_batches_.size(); ++b) {
        const auto& batch = right_batches_[b];
        std::vector<bool> matches = GetMatchingRows(batch, right_preds);
        auto col = batch->GetColumnByName(right_join_col);
        if (!col) continue;
        auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);

        for (size_t r = 0; r < static_cast<size_t>(batch->num_rows()); ++r) {
            if (matches[r] && !str_arr->IsNull(r)) {
                std::string_view val = str_arr->GetView(r);
                FatSignature sig{GenerateKimSignature(val), GenerateXorSum(val)};
                uint32_t packed_id = PackRowId(node_id_, static_cast<uint32_t>(b), static_cast<uint32_t>(r));
                local_right_signatures_.push_back({sig, packed_id});
            }
        }
    }
    std::cout << "[Node " << node_id_ << " Debug] Prepared signatures L=" 
              << local_left_signatures_.size() << " R=" << local_right_signatures_.size() << std::endl;
}

void ClusterNode::ShuffleLeftSignatures(const std::vector<std::pair<std::string, int>>& node_addresses) {
    received_left_signatures_.clear();

    // 1. Detect Skew in local Left table partition
    std::unordered_map<FatSignature, int> signature_counts;
    for (const auto& entry : local_left_signatures_) {
        signature_counts[entry.sig]++;
    }

    std::unordered_set<FatSignature> hot_signatures;
    for (const auto& pair : signature_counts) {
        if (pair.second > 16) {
            hot_signatures.insert(pair.first);
        }
    }

    // 2. Shuffle Left Signatures (Build Side)
    std::vector<std::vector<ShuffleEntry>> left_sends(num_nodes_);
    for (const auto& entry : local_left_signatures_) {
        if (hot_signatures.count(entry.sig)) {
            // Replicate hot keys to ALL nodes
            for (int n = 0; n < num_nodes_; ++n) {
                left_sends[n].push_back(entry);
            }
        } else {
            // Hashing partition for cold keys
            size_t target = (entry.sig.kim ^ entry.sig.xor_sum) % num_nodes_;
            left_sends[target].push_back(entry);
        }
    }

    for (int n = 0; n < num_nodes_; ++n) {
        if (!left_sends[n].empty()) {
            transport_.Send(node_addresses[n].first, node_addresses[n].second, left_sends[n]);
        }
    }
}

void ClusterNode::CollectLeftSignatures() {
    for (auto& msg : transport_.ReceiveAll()) {
        received_left_signatures_.insert(received_left_signatures_.end(), msg.begin(), msg.end());
    }
}

void ClusterNode::ShuffleRightSignatures(const std::vector<std::pair<std::string, int>>& node_addresses) {
    received_right_signatures_.clear();

    // 1. Detect Skew in local Left table partition (same hot keys detected)
    std::unordered_map<FatSignature, int> signature_counts;
    for (const auto& entry : local_left_signatures_) {
        signature_counts[entry.sig]++;
    }

    std::unordered_set<FatSignature> hot_signatures;
    for (const auto& pair : signature_counts) {
        if (pair.second > 16) {
            hot_signatures.insert(pair.first);
        }
    }

    // 2. Shuffle Right Signatures (Probe Side)
    std::vector<std::vector<ShuffleEntry>> right_sends(num_nodes_);
    for (const auto& entry : local_right_signatures_) {
        if (hot_signatures.count(entry.sig)) {
            // Distribute probe side of hot keys using Virtual Partition ID (batch index)
            uint32_t node_id, batch_idx, row_idx;
            UnpackRowId(entry.row_id, node_id, batch_idx, row_idx);
            size_t target = ((entry.sig.kim ^ entry.sig.xor_sum) + batch_idx) % num_nodes_;
            right_sends[target].push_back(entry);
        } else {
            // Standard hashing partition for cold keys
            size_t target = (entry.sig.kim ^ entry.sig.xor_sum) % num_nodes_;
            right_sends[target].push_back(entry);
        }
    }

    // Send Right Shuffles
    for (int n = 0; n < num_nodes_; ++n) {
        if (!right_sends[n].empty()) {
            std::cout << "[Node " << node_id_ << " Debug] Sending " << right_sends[n].size() 
                      << " Right signatures to Node " << n << std::endl;
            transport_.Send(node_addresses[n].first, node_addresses[n].second, right_sends[n]);
        }
    }
}

void ClusterNode::CollectRightSignatures() {
    for (auto& msg : transport_.ReceiveAll()) {
        received_right_signatures_.insert(received_right_signatures_.end(), msg.begin(), msg.end());
    }
}

std::vector<std::pair<uint32_t, uint32_t>> ClusterNode::PerformLocalJoin() {
    std::vector<std::pair<uint32_t, uint32_t>> matches;

    // Build side (received Left signatures)
    std::unordered_multimap<FatSignature, uint32_t> left_map;
    left_map.reserve(received_left_signatures_.size());
    for (const auto& entry : received_left_signatures_) {
        left_map.insert({entry.sig, entry.row_id});
    }

    // Probe side (received Right signatures)
    for (const auto& right_entry : received_right_signatures_) {
        auto range = left_map.equal_range(right_entry.sig);
        for (auto it = range.first; it != range.second; ++it) {
            matches.push_back({it->second, right_entry.row_id});
        }
    }
    std::cout << "[Node " << node_id_ << " Debug] Found local matches: " << matches.size() << std::endl;

    return matches;
}

std::string ClusterNode::GetLeftStringVal(uint32_t row_id, const std::string& col_name) const {
    uint32_t node_id, batch_idx, row_idx;
    UnpackRowId(row_id, node_id, batch_idx, row_idx);
    auto col = left_batches_[batch_idx]->GetColumnByName(col_name);
    if (!col) return "";
    auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
    return std::string(str_arr->GetView(row_idx));
}

std::string ClusterNode::GetRightStringVal(uint32_t row_id, const std::string& col_name) const {
    uint32_t node_id, batch_idx, row_idx;
    UnpackRowId(row_id, node_id, batch_idx, row_idx);
    auto col = right_batches_[batch_idx]->GetColumnByName(col_name);
    if (!col) return "";
    auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
    return std::string(str_arr->GetView(row_idx));
}

int64_t ClusterNode::GetLeftNumericVal(uint32_t row_id, const std::string& col_name) const {
    uint32_t node_id, batch_idx, row_idx;
    UnpackRowId(row_id, node_id, batch_idx, row_idx);
    auto col = left_batches_[batch_idx]->GetColumnByName(col_name);
    if (!col) return 0;
    auto int_arr = std::static_pointer_cast<arrow::Int64Array>(col);
    return int_arr->Value(row_idx);
}

int64_t ClusterNode::GetRightNumericVal(uint32_t row_id, const std::string& col_name) const {
    uint32_t node_id, batch_idx, row_idx;
    UnpackRowId(row_id, node_id, batch_idx, row_idx);
    auto col = right_batches_[batch_idx]->GetColumnByName(col_name);
    if (!col) return 0;
    auto int_arr = std::static_pointer_cast<arrow::Int64Array>(col);
    return int_arr->Value(row_idx);
}

// ==========================================
// DistributedCoordinator Implementation
// ==========================================

DistributedCoordinator::DistributedCoordinator(int num_nodes)
    : num_nodes_(num_nodes) {}

DistributedCoordinator::~DistributedCoordinator() {
    ShutdownCluster();
}

void DistributedCoordinator::InitializeCluster(const std::vector<std::pair<std::string, int>>& node_addresses) {
    node_addresses_ = node_addresses;
    nodes_.clear();
    for (int i = 0; i < num_nodes_; ++i) {
        nodes_.push_back(std::make_unique<ClusterNode>(i, node_addresses[i].first, node_addresses[i].second, num_nodes_));
        nodes_.back()->Start();
    }
}

void DistributedCoordinator::ShutdownCluster() {
    for (auto& node : nodes_) {
        node->Stop();
    }
    nodes_.clear();
}

void DistributedCoordinator::RegisterTable(const std::string& table_name, 
                                           const std::vector<std::string>& column_names, 
                                           const std::vector<uint32_t>& column_types) {
    // Simply proxy to dynamic metadata synchronizer backplane
    GlobalMetadata::Instance().RegisterTable(table_name, column_names, column_types);
}

void DistributedCoordinator::DistributeAndIngest(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch) {
    size_t total_rows = batch->num_rows();
    size_t rows_per_node = total_rows / num_nodes_;

    for (int n = 0; n < num_nodes_; ++n) {
        size_t start = n * rows_per_node;
        size_t length = (n == num_nodes_ - 1) ? (total_rows - start) : rows_per_node;

        if (length > 0) {
            auto slice = batch->Slice(static_cast<int64_t>(start), static_cast<int64_t>(length));
            if (table_name == "left" || table_name == "L") {
                nodes_[n]->IngestLeftPartition(slice);
            } else {
                nodes_[n]->IngestRightPartition(slice);
            }
        }
    }
}

uint64_t DistributedCoordinator::ExecuteJoinQuery(const std::string& left_table, const std::string& right_table,
                                                  const std::string& left_join_col, const std::string& right_join_col,
                                                  const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                                                  const std::string& agg_col, uint64_t& out_agg_sum) {
    out_agg_sum = 0;

    // 1. Prepare JIT filters and signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->PrepareJoinSignatures(left_table, right_table, left_preds, right_preds, left_join_col, right_join_col);
    }

    // 2. Perform global Left Signature Shuffle
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->ShuffleLeftSignatures(node_addresses_);
    }

    // Barrier: Wait for all Left shuffles to be fully received and reconstructed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Collect Left Signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->CollectLeftSignatures();
    }

    // 3. Perform global Right Signature Shuffle
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->ShuffleRightSignatures(node_addresses_);
    }

    // Barrier: Wait for all Right shuffles to be fully received and reconstructed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Collect Right Signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->CollectRightSignatures();
    }

    // 4. Local Hash Joins & Deferred String Equi-Join payload verification
    uint64_t total_matches = 0;
    for (int n = 0; n < num_nodes_; ++n) {
        auto local_matches = nodes_[n]->PerformLocalJoin();
        
        for (const auto& match : local_matches) {
            uint32_t left_row_id = match.first;
            uint32_t right_row_id = match.second;

            uint32_t l_node, l_batch, l_row;
            UnpackRowId(left_row_id, l_node, l_batch, l_row);

            uint32_t r_node, r_batch, r_row;
            UnpackRowId(right_row_id, r_node, r_batch, r_row);

            std::string l_val = nodes_[l_node]->GetLeftStringVal(left_row_id, left_join_col);
            std::string r_val = nodes_[r_node]->GetRightStringVal(right_row_id, right_join_col);

            if (l_val == r_val) {
                total_matches++;
                if (!agg_col.empty()) {
                    out_agg_sum += nodes_[l_node]->GetLeftNumericVal(left_row_id, agg_col);
                }
            }
        }
    }

    return total_matches;
}

QueryResult DistributedCoordinator::ExecuteJoinQueryExtended(const std::string& left_table, const std::string& right_table,
                                                             const std::string& left_join_col, const std::string& right_join_col,
                                                             const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                                                             const std::string& agg_col, AggregationType agg_type) {
    QueryResult res;
    res.count = 0;
    res.sum = 0;
    (void)agg_type;

    // 1. Prepare JIT filters and signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->PrepareJoinSignatures(left_table, right_table, left_preds, right_preds, left_join_col, right_join_col);
    }

    // 2. Perform global Left Signature Shuffle
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->ShuffleLeftSignatures(node_addresses_);
    }

    // Barrier: Wait for all Left shuffles to be fully received and reconstructed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Collect Left Signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->CollectLeftSignatures();
    }

    // 3. Perform global Right Signature Shuffle
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->ShuffleRightSignatures(node_addresses_);
    }

    // Barrier: Wait for all Right shuffles to be fully received and reconstructed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Collect Right Signatures on all nodes
    for (int n = 0; n < num_nodes_; ++n) {
        nodes_[n]->CollectRightSignatures();
    }

    // 4. Local Hash Joins & Deferred String Equi-Join payload verification
    std::vector<std::pair<uint32_t, uint32_t>> matched_pairs;
    for (int n = 0; n < num_nodes_; ++n) {
        auto local_matches = nodes_[n]->PerformLocalJoin();
        
        for (const auto& match : local_matches) {
            uint32_t left_row_id = match.first;
            uint32_t right_row_id = match.second;

            uint32_t l_node, l_batch, l_row;
            UnpackRowId(left_row_id, l_node, l_batch, l_row);

            uint32_t r_node, r_batch, r_row;
            UnpackRowId(right_row_id, r_node, r_batch, r_row);

            std::string l_val = nodes_[l_node]->GetLeftStringVal(left_row_id, left_join_col);
            std::string r_val = nodes_[r_node]->GetRightStringVal(right_row_id, right_join_col);

            if (l_val == r_val) {
                matched_pairs.push_back(match);
            }
        }
    }

    res.count = matched_pairs.size();
    if (res.count == 0 || agg_col.empty()) {
        return res;
    }

    // Determine type of agg_col
    auto state = GlobalMetadata::Instance().GetGlobalState(left_table);
    bool is_string = false;
    if (state) {
        for (size_t i = 0; i < state->column_names.size(); ++i) {
            if (state->column_names[i] == agg_col) {
                if (state->column_types[i] == 13) {
                    is_string = true;
                }
                break;
            }
        }
    }

    std::vector<int64_t> matched_numerics;
    std::vector<std::string> matched_strings;
    
    for (const auto& match : matched_pairs) {
        uint32_t left_row_id = match.first;
        uint32_t l_node, l_batch, l_row;
        UnpackRowId(left_row_id, l_node, l_batch, l_row);
        
        if (is_string) {
            std::string s_val = nodes_[l_node]->GetLeftStringVal(left_row_id, agg_col);
            matched_strings.push_back(s_val);
        } else {
            int64_t n_val = nodes_[l_node]->GetLeftNumericVal(left_row_id, agg_col);
            matched_numerics.push_back(n_val);
            res.sum += n_val;
        }
    }
    
    if (is_string) {
        if (!matched_strings.empty()) {
            std::string min_s = matched_strings[0];
            std::string max_s = matched_strings[0];
            for (const auto& s : matched_strings) {
                if (s < min_s) min_s = s;
                if (s > max_s) max_s = s;
            }
            res.min_val = 0;
            res.max_val = 0;
            std::unordered_set<std::string> dist_set(matched_strings.begin(), matched_strings.end());
            res.distinct_count = dist_set.size();
            res.distinct_strings.assign(dist_set.begin(), dist_set.end());
        }
    } else {
        if (!matched_numerics.empty()) {
            res.min_val = matched_numerics[0];
            res.max_val = matched_numerics[0];
            for (auto val : matched_numerics) {
                if (val < res.min_val) res.min_val = val;
                if (val > res.max_val) res.max_val = val;
            }
            
            std::unordered_set<int64_t> dist_set(matched_numerics.begin(), matched_numerics.end());
            res.distinct_count = dist_set.size();
            res.distinct_numerics.assign(dist_set.begin(), dist_set.end());
            
            res.avg = static_cast<double>(res.sum) / matched_numerics.size();
            
            std::vector<int64_t> sorted_n = matched_numerics;
            std::sort(sorted_n.begin(), sorted_n.end());
            size_t sz = sorted_n.size();
            if (sz % 2 == 1) {
                res.median_val = sorted_n[sz / 2];
            } else {
                res.median_val = (sorted_n[sz / 2 - 1] + sorted_n[sz / 2]) / 2;
            }
        }
    }

    return res;
}

std::string DistributedCoordinator::GetS3ExpressBucketAZ(const std::string& bucket_name) const {
    size_t start = bucket_name.find("--");
    if (start == std::string::npos) return "";
    size_t end = bucket_name.find("--x-s3", start + 2);
    if (end == std::string::npos) return "";
    return bucket_name.substr(start + 2, end - (start + 2)); // e.g. "use1-az4"
}

int DistributedCoordinator::ScheduleTask(const std::string& bucket_name) const {
    std::string bucket_az = GetS3ExpressBucketAZ(bucket_name);
    if (bucket_az.empty()) {
        return 0; // default node
    }
    for (int n = 0; n < num_nodes_; ++n) {
        std::string node_az = nodes_[n]->GetAvailabilityZone();
        size_t node_dash = node_az.find_last_of('-');
        size_t bucket_dash = bucket_az.find_last_of('-');
        std::string node_sub = (node_dash != std::string::npos) ? node_az.substr(node_dash + 1) : node_az;
        std::string bucket_sub = (bucket_dash != std::string::npos) ? bucket_az.substr(bucket_dash + 1) : bucket_az;
        if (node_sub == bucket_sub || node_az.find(bucket_az) != std::string::npos || bucket_az.find(node_az) != std::string::npos) {
            return n;
        }
    }
    return 0; // fallback
}

} // namespace greengate
