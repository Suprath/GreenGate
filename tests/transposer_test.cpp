#include "lex/ingest/simd_transposer.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include "lex/storage/exporter.hpp"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <cassert>
#include <cstring>

using namespace greengate;

// A simple local macro to run tests and assert
#define ASSERT_TRUE(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: " << #cond << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::exit(1); \
        } \
    } while (0)

void TestTransposerCorrectness() {
    std::cout << "[Test] Running Transposer Correctness..." << std::endl;
    
    // Generate random 64-bit values
    std::mt19937_64 rng(42);
    alignas(64) uint64_t input[64];
    for (int i = 0; i < 64; ++i) {
        input[i] = rng();
    }
    
    alignas(64) bool valid_mask[64];
    for (int i = 0; i < 64; ++i) {
        valid_mask[i] = (i % 3 != 0); // mixed validity
    }
    
    alignas(64) uint64_t out_planes[64];
    uint64_t out_validity = 0;
    
    SimdTransposer::Transpose64(input, valid_mask, out_planes, &out_validity);
    
    // Verify validity plane matches valid_mask
    uint64_t expected_validity = 0;
    for (int i = 0; i < 64; ++i) {
        if (valid_mask[i]) {
            expected_validity |= (1ULL << i);
        }
    }
    ASSERT_TRUE(out_validity == expected_validity);
    
    // Verify transposer correctness: bit(j) of input(i) should match bit(i) of plane(j)
    for (int i = 0; i < 64; ++i) {
        for (int j = 0; j < 64; ++j) {
            uint64_t input_bit = (input[i] >> j) & 1ULL;
            uint64_t plane_bit = (out_planes[j] >> i) & 1ULL;
            ASSERT_TRUE(input_bit == plane_bit);
        }
    }
    
    std::cout << "  -> Transposer Correctness Passed!" << std::endl;
}

void TestTransposerNullMasking() {
    std::cout << "[Test] Running Transposer Null Masking (nullptr)..." << std::endl;
    
    alignas(64) uint64_t input[64];
    std::memset(input, 0xAB, sizeof(input));
    
    alignas(64) uint64_t out_planes[64];
    uint64_t out_validity = 0;
    
    // With nullptr valid_mask, all bits in validity should be 1
    SimdTransposer::Transpose64(input, nullptr, out_planes, &out_validity);
    ASSERT_TRUE(out_validity == 0xFFFFFFFFFFFFFFFFULL);
    
    std::cout << "  -> Transposer Null Masking Passed!" << std::endl;
}

void TestIngesterAndExporter() {
    std::cout << "[Test] Running Ingester and Exporter Integration..." << std::endl;
    
    // Build an Arrow RecordBatch with:
    // - Int32 column (with nulls)
    // - Bool column (with nulls)
    // - String column (with nulls and empty strings)
    
    arrow::Int32Builder int_builder;
    arrow::BooleanBuilder bool_builder;
    arrow::StringBuilder string_builder;
    
    const size_t num_rows = 150; // More than 2 blocks of 64
    for (size_t i = 0; i < num_rows; ++i) {
        if (i % 5 == 0) {
            ASSERT_TRUE(int_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(int_builder.Append(static_cast<int32_t>(i * 10)).ok());
        }
        
        if (i % 7 == 0) {
            ASSERT_TRUE(bool_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(bool_builder.Append(i % 2 == 0).ok());
        }
        
        if (i % 10 == 0) {
            ASSERT_TRUE(string_builder.AppendNull().ok());
        } else if (i % 10 == 3) {
            ASSERT_TRUE(string_builder.Append("").ok());
        } else {
            std::string s = "str_" + std::to_string(i);
            ASSERT_TRUE(string_builder.Append(s).ok());
        }
    }
    
    std::shared_ptr<arrow::Array> int_arr;
    std::shared_ptr<arrow::Array> bool_arr;
    std::shared_ptr<arrow::Array> string_arr;
    
    ASSERT_TRUE(int_builder.Finish(&int_arr).ok());
    ASSERT_TRUE(bool_builder.Finish(&bool_arr).ok());
    ASSERT_TRUE(string_builder.Finish(&string_arr).ok());
    
    auto schema = arrow::schema({
        arrow::field("c_int", arrow::int32()),
        arrow::field("c_bool", arrow::boolean()),
        arrow::field("c_str", arrow::utf8())
    });
    
    auto batch = arrow::RecordBatch::Make(schema, num_rows, {int_arr, bool_arr, string_arr});
    
    // Ingest the RecordBatch
    AdaptiveIngester ingester;
    RowGroup rg = ingester.Ingest(batch);
    
    ASSERT_TRUE(rg.num_rows == num_rows);
    ASSERT_TRUE(rg.num_columns == 4);
    ASSERT_TRUE(rg.column_names[0] == "c_int");
    ASSERT_TRUE(rg.column_names[1] == "c_bool");
    ASSERT_TRUE(rg.column_names[2] == "c_str");
    ASSERT_TRUE(rg.column_names[3] == "c_str_kim");
    
    size_t expected_blocks = (num_rows + 63) / 64; // 3 blocks
    ASSERT_TRUE(rg.blocks.size() == expected_blocks);
    
    // Serialize
    std::vector<uint8_t> serialized = Exporter::Serialize(rg);
    ASSERT_TRUE(serialized.size() > 0);
    
    // Deserialize
    RowGroup deserialized = Exporter::Deserialize(serialized);
    
    // Verify match
    ASSERT_TRUE(deserialized.num_rows == rg.num_rows);
    ASSERT_TRUE(deserialized.num_columns == rg.num_columns);
    ASSERT_TRUE(deserialized.column_names == rg.column_names);
    ASSERT_TRUE(deserialized.column_types == rg.column_types);
    ASSERT_TRUE(deserialized.blocks.size() == rg.blocks.size());
    
    for (size_t b = 0; b < expected_blocks; ++b) {
        const auto& block_orig = rg.blocks[b];
        const auto& block_deser = deserialized.blocks[b];
        
        ASSERT_TRUE(block_orig.columns.size() == block_deser.columns.size());
        for (size_t c = 0; c < rg.num_columns; ++c) {
            const auto& tile_orig = block_orig.columns[c];
            const auto& tile_deser = block_deser.columns[c];
            
            ASSERT_TRUE(tile_orig.validity == tile_deser.validity);
            for (size_t p = 0; p < 64; ++p) {
                ASSERT_TRUE(tile_orig.planes[p] == tile_deser.planes[p]);
            }
        }
        
        ASSERT_TRUE(block_orig.tail_payload == block_deser.tail_payload);
    }
    
    std::cout << "  -> Ingester and Exporter Integration Passed!" << std::endl;
}

void BenchmarkTransposer() {
    std::cout << "[Benchmark] Benchmarking SIMD Transposer..." << std::endl;
    
    constexpr size_t num_blocks = 50000; // 3.2M rows
    std::vector<uint64_t> input(num_blocks * 64, 0x123456789ABCDEF0ULL);
    alignas(64) bool block_valid_mask[64];
    std::fill(block_valid_mask, block_valid_mask + 64, true);
    
    std::vector<uint64_t> out_planes(num_blocks * 64, 0);
    std::vector<uint64_t> out_validities(num_blocks, 0);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t b = 0; b < num_blocks; ++b) {
        SimdTransposer::Transpose64(
            &input[b * 64],
            block_valid_mask,
            &out_planes[b * 64],
            &out_validities[b]
        );
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    
    double elapsed_sec = diff.count();
    double total_mb = static_cast<double>(num_blocks * 64 * sizeof(uint64_t)) / (1024.0 * 1024.0);
    double throughput_gb_s = (total_mb / 1024.0) / elapsed_sec;
    double records_per_sec = static_cast<double>(num_blocks * 64) / elapsed_sec;
    
    std::cout << "  Processed: " << (num_blocks * 64) << " records (" << total_mb << " MB)" << std::endl;
    std::cout << "  Time: " << elapsed_sec << " seconds" << std::endl;
    std::cout << "  Throughput: " << throughput_gb_s << " GB/s" << std::endl;
    std::cout << "  Speed: " << (records_per_sec / 1e6) << " Million Records/sec (MRPS)" << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Starting GreenGate Phase 1 Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    
    TestTransposerCorrectness();
    TestTransposerNullMasking();
    TestIngesterAndExporter();
    BenchmarkTransposer();
    
    std::cout << "========================================" << std::endl;
    std::cout << "All GreenGate Phase 1 Tests Passed Successfully!" << std::endl;
    std::cout << "========================================" << std::endl;
    return 0;
}
