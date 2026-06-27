#include "lex/lsm/metadata_registry.hpp"
#include "lex/lsm/promotion_daemon.hpp"
#include "lex/jit/query_planner.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <thread>

using namespace greengate;

#define ASSERT_TRUE(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: " << #cond << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::exit(1); \
        } \
    } while (0)

#include <signal.h>
#include <execinfo.h>

void handler(int sig) {
    void *array[10];
    size_t size;
    size = backtrace(array, 10);
    fprintf(stderr, "Error: signal %d:\n", sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}

static std::shared_ptr<arrow::RecordBatch> CreateBatch(size_t start_idx, size_t num_rows, bool inject_nulls = false) {
    std::cout << "    [CreateBatch] Entering with start_idx=" << start_idx << ", num_rows=" << num_rows << std::endl;
    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;
    for (size_t i = 0; i < num_rows; ++i) {
        size_t idx = start_idx + i;
        if (inject_nulls && idx % 5 == 0) {
            ASSERT_TRUE(str_builder.AppendNull().ok());
        } else {
            std::string s = (idx % 2 == 0) ? "alpha" : "beta";
            ASSERT_TRUE(str_builder.Append(s).ok());
        }
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
    std::cout << "    [CreateBatch] Creating RecordBatch..." << std::endl;
    auto batch = arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});
    std::cout << "    [CreateBatch] Returning RecordBatch" << std::endl;
    return batch;
}

void TestLsmDynamicWritesAndQueries() {
    std::cout << "[Test] Running LSM Dynamic Writes and Queries..." << std::endl;

    // 1. Register table
    std::cout << "  Registering table..." << std::endl;
    MetadataRegistry::Instance().RegisterTable("users", {"c_str", "c_val"}, {13, 0});
    std::cout << "  Getting table state..." << std::endl;
    auto state = MetadataRegistry::Instance().GetTableState("users");
    ASSERT_TRUE(state != nullptr);
    ASSERT_TRUE(state->active_memtable != nullptr);

    // 2. Append some records to active memtable
    std::cout << "  Calling CreateBatch..." << std::endl;
    auto batch = CreateBatch(0, 100);
    std::cout << "  Inserting batch..." << std::endl;
    MetadataRegistry::Instance().InsertBatch("users", batch);
    std::cout << "  Batch inserted." << std::endl;

    // Get updated state
    state = MetadataRegistry::Instance().GetTableState("users");
    ASSERT_TRUE(state->active_memtable->GetRowCount() == 100);

    // 3. Execute query on the TableState.
    // Query: c_str = "alpha" AND c_val > 100. Aggregation on c_val.
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "alpha", 0, true},
        {"c_val", PredicateOp::GT, "", 100, false}
    };

    AdaptiveIngester ingester;
    auto dummy_batch = CreateBatch(0, 1);
    RowGroup dummy_rg = ingester.Ingest(dummy_batch);

    QueryRunner runner = QueryPlanner::Plan(preds, dummy_rg, "c_val");

    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(*state, agg_sum);

    uint64_t expected_matches = 0;
    uint64_t expected_sum = 0;
    for (size_t idx = 0; idx < 100; ++idx) {
        std::string s = (idx % 2 == 0) ? "alpha" : "beta";
        int64_t val = idx * 10;
        if (s == "alpha" && val > 100) {
            expected_matches++;
            expected_sum += val;
        }
    }

    std::cout << "  Active MemTable matches: " << matches << " (Expected: " << expected_matches << ")" << std::endl;
    std::cout << "  Active MemTable agg_sum: " << agg_sum << " (Expected: " << expected_sum << ")" << std::endl;
    ASSERT_TRUE(matches == expected_matches);
    ASSERT_TRUE(agg_sum == expected_sum);

    std::cout << "  -> LSM Dynamic Writes and Queries Passed!" << std::endl;
}

void TestLsmDeletes() {
    std::cout << "[Test] Running LSM Tombstone Deletes..." << std::endl;

    // Register a new table
    MetadataRegistry::Instance().RegisterTable("users_deletes", {"c_str", "c_val"}, {13, 0});
    auto state = MetadataRegistry::Instance().GetTableState("users_deletes");

    // Ingest a batch with 128 rows (2 blocks of 64)
    auto batch = CreateBatch(0, 128);
    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);
    
    // Add to persisted groups
    auto new_state = std::make_shared<TableState>();
    new_state->table_name = state->table_name;
    new_state->column_names = state->column_names;
    new_state->column_types = state->column_types;
    new_state->active_memtable = state->active_memtable;
    new_state->persisted_groups.push_back(std::make_shared<RowGroup>(rg));
    new_state->version = state->version + 1;
    MetadataRegistry::Instance().UpdateTableState("users_deletes", new_state);

    // Query: c_str = "alpha"
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "alpha", 0, true}
    };
    
    QueryRunner runner = QueryPlanner::Plan(preds, rg);
    
    state = MetadataRegistry::Instance().GetTableState("users_deletes");
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(*state, agg_sum);
    ASSERT_TRUE(matches == 64);

    // Delete row 4 (even index, c_str = "alpha") and row 5 (odd index, c_str = "beta")
    MetadataRegistry::Instance().DeleteRow("users_deletes", 4);
    MetadataRegistry::Instance().DeleteRow("users_deletes", 5);

    // Query again
    state = MetadataRegistry::Instance().GetTableState("users_deletes");
    matches = runner.Execute(*state, agg_sum);
    
    std::cout << "  Matches after deletes: " << matches << " (Expected: 63)" << std::endl;
    ASSERT_TRUE(matches == 63);

    std::cout << "  -> LSM Tombstone Deletes Passed!" << std::endl;
}

void TestLsmCompaction() {
    std::cout << "[Test] Running LSM Compaction..." << std::endl;

    // Register table
    MetadataRegistry::Instance().RegisterTable("users_compaction", {"c_str", "c_val"}, {13, 0});
    auto state = MetadataRegistry::Instance().GetTableState("users_compaction");

    // 1. Add some persisted rows (64 rows)
    auto batch1 = CreateBatch(0, 64, true);
    AdaptiveIngester ingester;
    RowGroup rg1 = ingester.Ingest(batch1);
    
    auto state2 = std::make_shared<TableState>();
    state2->table_name = state->table_name;
    state2->column_names = state->column_names;
    state2->column_types = state->column_types;
    state2->active_memtable = state->active_memtable;
    state2->persisted_groups.push_back(std::make_shared<RowGroup>(rg1));
    state2->version = state->version + 1;
    MetadataRegistry::Instance().UpdateTableState("users_compaction", state2);

    // 2. Add some active memtable rows (another 64 rows)
    auto batch2 = CreateBatch(64, 64, true);
    MetadataRegistry::Instance().InsertBatch("users_compaction", batch2);

    // 3. Mark some persisted rows as deleted
    // Row 4 (even index: c_str = "alpha") and Row 6 (even index: c_str = "alpha")
    MetadataRegistry::Instance().DeleteRow("users_compaction", 4);
    MetadataRegistry::Instance().DeleteRow("users_compaction", 6);

    // Let's check initial query before compaction
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "alpha", 0, true}
    };
    QueryRunner runner = QueryPlanner::Plan(preds, rg1);
    
    state = MetadataRegistry::Instance().GetTableState("users_compaction");
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(*state, agg_sum);
    std::cout << "  Matches before compaction: " << matches << " (Expected: 49)" << std::endl;
    ASSERT_TRUE(matches == 49);

    // 4. Trigger compaction using PromotionDaemon
    PromotionDaemon daemon("users_compaction", 1024 * 1024);
    std::cout << "  Triggering manual compaction..." << std::endl;
    daemon.TriggerCompaction();

    // 5. Verify the new state
    state = MetadataRegistry::Instance().GetTableState("users_compaction");
    
    ASSERT_TRUE(state->active_memtable->GetRowCount() == 0);
    ASSERT_TRUE(state->frozen_memtable == nullptr);
    ASSERT_TRUE(state->persisted_groups.size() == 1);
    
    std::cout << "  Compacted RowGroup num_rows: " << state->persisted_groups[0]->num_rows << " (Expected: 126)" << std::endl;
    ASSERT_TRUE(state->persisted_groups[0]->num_rows == 126);

    uint64_t agg_sum2 = 0;
    uint64_t matches2 = runner.Execute(*state, agg_sum2);
    std::cout << "  Matches after compaction: " << matches2 << " (Expected: 49)" << std::endl;
    ASSERT_TRUE(matches2 == 49);

    std::cout << "  -> LSM Compaction Passed!" << std::endl;
}

int main() {
    signal(SIGSEGV, handler);
    std::cout << "===========================================" << std::endl;
    std::cout << "Running Phase 3 LSM integration tests..." << std::endl;
    std::cout << "===========================================" << std::endl;

    TestLsmDynamicWritesAndQueries();
    TestLsmDeletes();
    TestLsmCompaction();

    std::cout << "===========================================" << std::endl;
    std::cout << "All LSM integration tests PASSED successfully!" << std::endl;
    std::cout << "===========================================" << std::endl;
    return 0;
}
