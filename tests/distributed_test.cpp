#include "lex/distributed/distributed_coordinator.hpp"
#include "lex/distributed/global_metadata.hpp"
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <numeric>

using namespace greengate;

#define ASSERT_TRUE(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: " << #cond << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::exit(1); \
        } \
    } while (0)

static std::shared_ptr<arrow::RecordBatch> CreateJoinBatch(size_t start_idx, size_t num_rows, bool inject_skew) {
    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;
    
    for (size_t i = 0; i < num_rows; ++i) {
        size_t idx = start_idx + i;
        std::string s;
        if (inject_skew && i < 30) {
            // Skew: first 30 rows have the exact same key "hot_user"
            s = "hot_user";
        } else {
            s = "user_" + std::to_string(idx);
        }
        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(idx * 10)).ok());
    }
    
    std::shared_ptr<arrow::Array> str_arr;
    std::shared_ptr<arrow::Array> int_arr;
    ASSERT_TRUE(str_builder.Finish(&str_arr).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());
    
    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });
    return arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});
}

void TestDistributedJoinAndSkewHandling() {
    std::cout << "[Test] Running Distributed Join and Skew Handling..." << std::endl;

    const int num_nodes = 4;
    std::vector<std::pair<std::string, int>> node_addresses = {
        {"127.0.0.1", 9001},
        {"127.0.0.1", 9002},
        {"127.0.0.1", 9003},
        {"127.0.0.1", 9004}
    };

    DistributedCoordinator coordinator(num_nodes);
    coordinator.InitializeCluster(node_addresses);

    // Register tables globally
    GlobalMetadata::Instance().Clear();
    coordinator.RegisterTable("L", {"c_str", "c_val"}, {13, 0});
    coordinator.RegisterTable("R", {"c_str", "c_val"}, {13, 0});

    // Create Left and Right datasets
    auto left_batch = CreateJoinBatch(0, 200, true);
    auto right_batch = CreateJoinBatch(0, 200, true);

    // Distribute data partitions to nodes
    coordinator.DistributeAndIngest("L", left_batch);
    coordinator.DistributeAndIngest("R", right_batch);

    // Predicates and query configurations
    // L.c_val > 100 AND R.c_val > 150
    std::vector<Predicate> left_preds = {
        {"c_val", PredicateOp::GT, "", 100, false}
    };
    std::vector<Predicate> right_preds = {
        {"c_val", PredicateOp::GT, "", 150, false}
    };

    // Reset traffic stats prior to running the query
    for (int n = 0; n < num_nodes; ++n) {
        coordinator.GetNodes()[n]->GetTransport().ResetStats();
    }

    uint64_t agg_sum = 0;
    uint64_t matches = coordinator.ExecuteJoinQuery("L", "R", "c_str", "c_str", left_preds, right_preds, "c_val", agg_sum);

    // Check stats
    uint64_t total_sent = 0;
    uint64_t total_received = 0;
    for (int n = 0; n < num_nodes; ++n) {
        total_sent += coordinator.GetNodes()[n]->GetTransport().GetTotalBytesSent();
        total_received += coordinator.GetNodes()[n]->GetTransport().GetTotalBytesReceived();
    }

    // Expected matches calculation:
    // Left: 30 "hot_user" rows (indices 0..29). Filter val > 100 leaves indices 11..29 (19 rows).
    // Right: 30 "hot_user" rows (indices 0..29). Filter val > 150 leaves indices 16..29 (14 rows).
    // Skew matches: 19 * 14 = 266.
    // Left c_val sum for skew matches: 14 * sum(110, 120, ..., 290) = 14 * 3800 = 53200.
    // 
    // Cold unique matches: indices 30..199 (170 rows). Since both Left and Right values > 150, all 170 rows match.
    // Cold matches: 170.
    // Left c_val sum for cold matches: sum(300, 310, ..., 1990) = 194650.
    // 
    // Total expected matches = 266 + 170 = 436.
    // Total expected sum = 53200 + 194650 = 247850.
    std::cout << "  Distributed Join matches: " << matches << " (Expected: 436)" << std::endl;
    std::cout << "  Distributed Join agg_sum: " << agg_sum << " (Expected: 247850)" << std::endl;

    ASSERT_TRUE(matches == 436);
    ASSERT_TRUE(agg_sum == 247850);

    // Calculate baseline traffic if shuffling raw string keys instead of signatures
    // Suppose avg string key size is 12 bytes. With 4-byte row_id, that is 16 bytes.
    // Our Signature Shuffle payload shuffles 16-byte FatSignature + 4-byte row_id + header/packet overhead.
    // The main reduction comes from not sending variable length payload and doing deferred materialization,
    // avoiding serializing full schema columns.
    std::cout << "  Total network bytes sent: " << total_sent << " bytes." << std::endl;
    std::cout << "  Total network bytes received: " << total_received << " bytes." << std::endl;
    ASSERT_TRUE(total_sent > 0);

    coordinator.ShutdownCluster();
    std::cout << "  -> Distributed Join and Skew Handling Passed!" << std::endl;
}

int main() {
    std::cout << "===========================================" << std::endl;
    std::cout << "Running Phase 4 Distributed scaling tests..." << std::endl;
    std::cout << "===========================================" << std::endl;

    TestDistributedJoinAndSkewHandling();

    std::cout << "===========================================" << std::endl;
    std::cout << "All Phase 4 Distributed scaling tests PASSED!" << std::endl;
    std::cout << "===========================================" << std::endl;
    return 0;
}
