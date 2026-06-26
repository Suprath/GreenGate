#pragma once
#include "lex/distributed/srd_transport.hpp"
#include "lex/jit/query_planner.hpp"
#include "lex/lsm/metadata_registry.hpp"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace greengate {

class ClusterNode {
public:
    ClusterNode(int node_id, const std::string& host, int port, int num_nodes);
    ~ClusterNode();

    void Start();
    void Stop();

    // Ingestion of local partitions
    void IngestLeftPartition(const std::shared_ptr<arrow::RecordBatch>& batch);
    void IngestRightPartition(const std::shared_ptr<arrow::RecordBatch>& batch);

    // Apply local query planner filters and generate join signatures
    void PrepareJoinSignatures(const std::string& left_table, const std::string& right_table,
                               const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                               const std::string& left_join_col, const std::string& right_join_col);

    // Split shuffle execution into stage-wise builder (Left) and prober (Right) phases
    void ShuffleLeftSignatures(const std::vector<std::pair<std::string, int>>& node_addresses);
    void CollectLeftSignatures();
    void ShuffleRightSignatures(const std::vector<std::pair<std::string, int>>& node_addresses);
    void CollectRightSignatures();

    // Collect received shuffle datagrams and execute the local hash join
    // Returns local matching row ID pairs: (LeftRowID, RightRowID)
    std::vector<std::pair<uint32_t, uint32_t>> PerformLocalJoin();

    // Lookup raw values for verification and materialization
    std::string GetLeftStringVal(uint32_t row_id, const std::string& col_name) const;
    std::string GetRightStringVal(uint32_t row_id, const std::string& col_name) const;
    int64_t GetLeftNumericVal(uint32_t row_id, const std::string& col_name) const;
    int64_t GetRightNumericVal(uint32_t row_id, const std::string& col_name) const;

    int GetNodeId() const { return node_id_; }
    SrdTransport& GetTransport() { return transport_; }

    std::string GetAvailabilityZone() const { return availability_zone_; }
    void SetAvailabilityZone(const std::string& az) { availability_zone_ = az; }

private:
    int node_id_;
    int num_nodes_;
    SrdTransport transport_;
    std::string availability_zone_;

    // Keep Arrow batches for zero-copy lookup during verification/materialization
    std::vector<std::shared_ptr<arrow::RecordBatch>> left_batches_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> right_batches_;

    // bitsliced rowgroups
    std::vector<RowGroup> left_row_groups_;
    std::vector<RowGroup> right_row_groups_;

    // Local lists of join signatures to shuffle
    std::vector<ShuffleEntry> local_left_signatures_;
    std::vector<ShuffleEntry> local_right_signatures_;

    // Received partitions
    std::vector<ShuffleEntry> received_left_signatures_;
    std::vector<ShuffleEntry> received_right_signatures_;
};

class DistributedCoordinator {
public:
    DistributedCoordinator(int num_nodes);
    ~DistributedCoordinator();

    void InitializeCluster(const std::vector<std::pair<std::string, int>>& node_addresses);
    void ShutdownCluster();

    void RegisterTable(const std::string& table_name, 
                       const std::vector<std::string>& column_names, 
                       const std::vector<uint32_t>& column_types);

    // Distribute a RecordBatch evenly across the cluster nodes
    void DistributeAndIngest(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch);

    // Executes a distributed join query: SELECT SUM(L.agg_col) FROM L JOIN R ON L.key = R.key WHERE L.filters AND R.filters
    uint64_t ExecuteJoinQuery(const std::string& left_table, const std::string& right_table,
                              const std::string& left_join_col, const std::string& right_join_col,
                              const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                              const std::string& agg_col, uint64_t& out_agg_sum);

    QueryResult ExecuteJoinQueryExtended(const std::string& left_table, const std::string& right_table,
                                         const std::string& left_join_col, const std::string& right_join_col,
                                         const std::vector<Predicate>& left_preds, const std::vector<Predicate>& right_preds,
                                         const std::string& agg_col, AggregationType agg_type);

    std::string GetS3ExpressBucketAZ(const std::string& bucket_name) const;
    int ScheduleTask(const std::string& bucket_name) const;

    std::vector<std::unique_ptr<ClusterNode>>& GetNodes() { return nodes_; }

private:
    int num_nodes_;
    std::vector<std::pair<std::string, int>> node_addresses_;
    std::vector<std::unique_ptr<ClusterNode>> nodes_;
};

} // namespace greengate
