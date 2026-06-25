#include "lex/ingest/simd_transposer.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include "lex/jit/query_planner.hpp"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>
#include <cstring>
#include <numeric>

using namespace greengate;

#define ASSERT_TRUE(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: " << #cond << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::exit(1); \
        } \
    } while (0)

void TestJitStringExactMatch() {
    std::cout << "[Test] Running JIT String Exact Match..." << std::endl;

    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;

    const size_t num_rows = 200;
    std::vector<std::string> ref_strings;
    std::vector<int64_t> ref_vals;

    for (size_t i = 0; i < num_rows; ++i) {
        std::string s;
        if (i % 5 == 0) s = "error";
        else if (i % 5 == 1) s = "warning";
        else if (i % 5 == 2) s = "fatal_error";
        else if (i % 5 == 3) s = "info";
        else s = "debug";

        ref_strings.push_back(s);
        ref_vals.push_back(static_cast<int64_t>(i * 5));

        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(i * 5)).ok());
    }

    std::shared_ptr<arrow::Array> str_arr;
    std::shared_ptr<arrow::Array> int_arr;
    ASSERT_TRUE(str_builder.Finish(&str_arr).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());

    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });

    auto batch = arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});

    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);

    // Predicates: c_str = 'error'
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "error", 0, true}
    };

    QueryRunner runner = QueryPlanner::Plan(preds, rg);
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(rg, agg_sum);

    // Compute expected matches
    uint64_t expected_matches = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        if (ref_strings[i] == "error") {
            expected_matches++;
        }
    }

    std::cout << "  Matches: " << matches << " (Expected: " << expected_matches << ")" << std::endl;
    ASSERT_TRUE(matches == expected_matches);
    std::cout << "  -> JIT String Exact Match Passed!" << std::endl;
}

void TestJitStringPrefixMatch() {
    std::cout << "[Test] Running JIT String Prefix Match..." << std::endl;

    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;

    const size_t num_rows = 200;
    std::vector<std::string> ref_strings;
    
    for (size_t i = 0; i < num_rows; ++i) {
        std::string s;
        if (i % 4 == 0) s = "fatal_error";
        else if (i % 4 == 1) s = "warning";
        else if (i % 4 == 2) s = "fatal_exception";
        else s = "info";

        ref_strings.push_back(s);
        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(i)).ok());
    }

    std::shared_ptr<arrow::Array> str_arr;
    std::shared_ptr<arrow::Array> int_arr;
    ASSERT_TRUE(str_builder.Finish(&str_arr).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());

    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });

    auto batch = arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});

    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);

    // Predicates: c_str LIKE 'fatal%'
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::LIKE_PREFIX, "fatal%", 0, true}
    };

    QueryRunner runner = QueryPlanner::Plan(preds, rg);
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(rg, agg_sum);

    uint64_t expected_matches = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        if (ref_strings[i].compare(0, 5, "fatal") == 0) {
            expected_matches++;
        }
    }

    std::cout << "  Matches: " << matches << " (Expected: " << expected_matches << ")" << std::endl;
    ASSERT_TRUE(matches == expected_matches);
    std::cout << "  -> JIT String Prefix Match Passed!" << std::endl;
}

void TestJitStringContainsMatch() {
    std::cout << "[Test] Running JIT String Substring Match..." << std::endl;

    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;

    const size_t num_rows = 200;
    std::vector<std::string> ref_strings;
    
    for (size_t i = 0; i < num_rows; ++i) {
        std::string s;
        if (i % 4 == 0) s = "error";
        else if (i % 4 == 1) s = "warning_msg";
        else if (i % 4 == 2) s = "error";
        else s = "info";

        ref_strings.push_back(s);
        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(i)).ok());
    }

    std::shared_ptr<arrow::Array> str_arr;
    std::shared_ptr<arrow::Array> int_arr;
    ASSERT_TRUE(str_builder.Finish(&str_arr).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());

    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });

    auto batch = arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});

    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);

    // Predicates: c_str LIKE '%error%'
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::LIKE_CONTAINS, "%error%", 0, true}
    };

    QueryRunner runner = QueryPlanner::Plan(preds, rg);
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(rg, agg_sum);

    uint64_t expected_matches = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        if (ref_strings[i].find("error") != std::string::npos) {
            expected_matches++;
        }
    }

    std::cout << "  Matches: " << matches << " (Expected: " << expected_matches << ")" << std::endl;
    ASSERT_TRUE(matches == expected_matches);
    std::cout << "  -> JIT String Substring Match Passed!" << std::endl;
}

void TestJitNumericAndAggregation() {
    std::cout << "[Test] Running JIT Numeric filter & In-Register SUM Aggregation..." << std::endl;

    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;

    const size_t num_rows = 200;
    std::vector<std::string> ref_strings;
    std::vector<int64_t> ref_vals;

    for (size_t i = 0; i < num_rows; ++i) {
        std::string s = (i % 2 == 0) ? "error" : "info";
        ref_strings.push_back(s);
        ref_vals.push_back(static_cast<int64_t>(i * 10));

        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(i * 10)).ok());
    }

    std::shared_ptr<arrow::Array> str_arr;
    std::shared_ptr<arrow::Array> int_arr;
    ASSERT_TRUE(str_builder.Finish(&str_arr).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());

    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });

    auto batch = arrow::RecordBatch::Make(schema, num_rows, {str_arr, int_arr});

    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);

    std::cout << "[Test Debug] Reconstructed c_val Block 0 values: ";
    alignas(64) uint64_t recon[64];
    greengate_butterfly_transpose(rg.blocks[0].columns[2].planes, recon);
    for (int i = 0; i < 5; ++i) {
        std::cout << recon[i] << " ";
    }
    std::cout << std::endl;

    // Predicates: c_str = 'error' AND c_val > 500
    // Select SUM(c_val)
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "error", 0, true},
        {"c_val", PredicateOp::GT, "", 500, false}
    };

    QueryRunner runner = QueryPlanner::Plan(preds, rg, "c_val");
    uint64_t agg_sum = 0;
    uint64_t matches = runner.Execute(rg, agg_sum);

    uint64_t expected_matches = 0;
    uint64_t expected_sum = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        if (ref_strings[i] == "error" && ref_vals[i] > 500) {
            expected_matches++;
            expected_sum += ref_vals[i];
        }
    }

    std::cout << "  Matches: " << matches << " (Expected: " << expected_matches << ")" << std::endl;
    std::cout << "  Aggregated Sum: " << agg_sum << " (Expected: " << expected_sum << ")" << std::endl;
    
    ASSERT_TRUE(matches == expected_matches);
    ASSERT_TRUE(agg_sum == expected_sum);
    std::cout << "  -> JIT Numeric filter & SUM Aggregation Passed!" << std::endl;
}

void BenchmarkJitComplexQuery() {
    std::cout << "[Benchmark] Benchmarking Complex JIT Scan with String predicate and Numeric Aggregation..." << std::endl;

    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;

    constexpr size_t num_rows = 1000000; // 1 Million rows
    
    for (size_t i = 0; i < 64; ++i) {
        std::string s = (i % 2 == 0) ? "error" : "info";
        ASSERT_TRUE(str_builder.Append(s).ok());
        ASSERT_TRUE(int_builder.Append(static_cast<int64_t>(i * 50)).ok());
    }

    std::shared_ptr<arrow::Array> str_arr_base;
    std::shared_ptr<arrow::Array> int_arr_base;
    ASSERT_TRUE(str_builder.Finish(&str_arr_base).ok());
    ASSERT_TRUE(int_builder.Finish(&int_arr_base).ok());

    // Replicate arrays to reach 1M rows quickly
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    size_t rows_generated = 0;
    auto schema = arrow::schema({
        arrow::field("c_str", arrow::utf8()),
        arrow::field("c_val", arrow::int64())
    });

    while (rows_generated < num_rows) {
        size_t chunk = std::min(size_t(64000), num_rows - rows_generated);
        arrow::StringBuilder str_chunk;
        arrow::Int64Builder int_chunk;
        for (size_t i = 0; i < chunk; ++i) {
            std::string s = (i % 2 == 0) ? "error" : "info";
            ASSERT_TRUE(str_chunk.Append(s).ok());
            ASSERT_TRUE(int_chunk.Append(static_cast<int64_t>(i * 50)).ok());
        }
        std::shared_ptr<arrow::Array> sa;
        std::shared_ptr<arrow::Array> ia;
        ASSERT_TRUE(str_chunk.Finish(&sa).ok());
        ASSERT_TRUE(int_chunk.Finish(&ia).ok());
        batches.push_back(arrow::RecordBatch::Make(schema, chunk, {sa, ia}));
        rows_generated += chunk;
    }

    // Ingest into a single RowGroup
    AdaptiveIngester ingester;
    RowGroup rg;
    rg.num_rows = 0;
    rg.num_columns = 0;
    
    for (const auto& batch : batches) {
        RowGroup sub_rg = ingester.Ingest(batch);
        if (rg.num_columns == 0) {
            rg.num_columns = sub_rg.num_columns;
            rg.column_names = sub_rg.column_names;
            rg.column_types = sub_rg.column_types;
        }
        rg.num_rows += sub_rg.num_rows;
        rg.blocks.insert(rg.blocks.end(), sub_rg.blocks.begin(), sub_rg.blocks.end());
    }

    std::cout << "  Ingested " << rg.num_rows << " rows (" << rg.blocks.size() << " blocks)." << std::endl;

    // Predicates: c_str = 'error' AND c_val > 1000
    // Select SUM(c_val)
    std::vector<Predicate> preds = {
        {"c_str", PredicateOp::EQ, "error", 0, true},
        {"c_val", PredicateOp::GT, "", 1000, false}
    };

    QueryRunner runner = QueryPlanner::Plan(preds, rg, "c_val");

    double max_mrps = 0.0;
    uint64_t agg_sum = 0;
    uint64_t matches = 0;
    double best_elapsed_sec = 0.0;

    for (int run = 0; run < 5; ++run) {
        uint64_t temp_sum = 0;
        auto start = std::chrono::high_resolution_clock::now();
        uint64_t temp_matches = runner.Execute(rg, temp_sum);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;
        double elapsed_sec = diff.count();
        double records_per_sec = static_cast<double>(num_rows) / elapsed_sec;
        double mrps = records_per_sec / 1e6;

        if (mrps > max_mrps) {
            max_mrps = mrps;
            agg_sum = temp_sum;
            matches = temp_matches;
            best_elapsed_sec = elapsed_sec;
        }
    }

    std::cout << "  Matches: " << matches << ", SUM: " << agg_sum << std::endl;
    std::cout << "  Best Scan Time: " << best_elapsed_sec * 1000.0 << " ms" << std::endl;
    std::cout << "  Max Scan Speed: " << max_mrps << " Million Records/sec (MRPS)" << std::endl;

    // Performance target validation: >400 MRPS
    ASSERT_TRUE(max_mrps > 400.0);
    std::cout << "  -> Performance Target Met (>400 MRPS)!" << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Starting GreenGate Phase 2 Tests" << std::endl;
    std::cout << "========================================" << std::endl;

    TestJitStringExactMatch();
    TestJitStringPrefixMatch();
    TestJitStringContainsMatch();
    TestJitNumericAndAggregation();
    BenchmarkJitComplexQuery();

    std::cout << "========================================" << std::endl;
    std::cout << "All GreenGate Phase 2 Tests Passed Successfully!" << std::endl;
    std::cout << "========================================" << std::endl;
    return 0;
}
