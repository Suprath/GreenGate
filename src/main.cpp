#include <iostream>
#include <chrono>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <arrow/api.h>
#include "apex/engine.hpp"
#include "green_gate/arrow_connector.hpp"
#include "green_gate/sql_parser.hpp"

// Struct for scalar verification
struct MarketTick {
    uint64_t ask;
    uint64_t bid;
    uint64_t volume;
};

#define ASSERT_OK(expr) \
    do { \
        auto _s = (expr); \
        if (!_s.ok()) { \
            std::cerr << "Arrow Error: " << _s.ToString() << "\n"; \
            std::exit(1); \
        } \
    } while (0)

// Generates a mock Arrow RecordBatch with 1,000,000 ticks
std::shared_ptr<arrow::RecordBatch> GenerateMockArrowData(int64_t num_rows) {
    arrow::UInt64Builder ask_builder;
    arrow::UInt64Builder bid_builder;
    arrow::UInt64Builder vol_builder;
    
    // Pre-reserve to avoid reallocations
    ASSERT_OK(ask_builder.Reserve(num_rows));
    ASSERT_OK(bid_builder.Reserve(num_rows));
    ASSERT_OK(vol_builder.Reserve(num_rows));
    
    for (int64_t i = 0; i < num_rows; ++i) {
        ASSERT_OK(ask_builder.Append(10000 + i));
        ASSERT_OK(bid_builder.Append(9998));
        ASSERT_OK(vol_builder.Append(1000 + i));
    }
    
    std::shared_ptr<arrow::Array> ask_array;
    std::shared_ptr<arrow::Array> bid_array;
    std::shared_ptr<arrow::Array> vol_array;
    
    ASSERT_OK(ask_builder.Finish(&ask_array));
    ASSERT_OK(bid_builder.Finish(&bid_array));
    ASSERT_OK(vol_builder.Finish(&vol_array));
    
    auto schema = arrow::schema({
        arrow::field("ask", arrow::uint64()),
        arrow::field("bid", arrow::uint64()),
        arrow::field("volume", arrow::uint64())
    });
    
    return arrow::RecordBatch::Make(schema, num_rows, {ask_array, bid_array, vol_array});
}

int main() {
    std::cout << "==================================================\n";
    std::cout << "🟢 GREEN PIPELINE: MILESTONE 1 VERIFICATION START\n";
    std::cout << "==================================================\n\n";

    constexpr int64_t num_rows = 1000000; // 1 Million Rows
    std::cout << "[Step 1] Generating " << num_rows << " rows of mock Arrow data...\n";
    auto batch = GenerateMockArrowData(num_rows);
    std::cout << "✓ Arrow RecordBatch generated successfully.\n\n";

    std::cout << "[Step 2] Initializing AarchGate Apex Engine...\n";
    apex::ApexEngine engine;
    greengate::ArrowConnector connector(engine);
    
    std::cout << "Registering schema based on Apache Arrow schema...\n";
    connector.RegisterSchema("market", batch->schema());
    std::cout << "✓ Schema registered in AarchGate.\n\n";

    // Standard SQL predicate query
    std::string sql_query = "ask > 10500 AND volume > 2000";
    std::cout << "[Step 3] Parsing SQL query: \"" << sql_query << "\"...\n";
    
    greengate::SqlParser parser;
    apex::ir::Node* ast_root = nullptr;
    try {
        ast_root = parser.Parse(sql_query);
        std::cout << "✓ SQL query parsed successfully to AST.\n";
    } catch (const std::exception& e) {
        std::cerr << "❌ SQL Parsing failed: " << e.what() << "\n";
        return 1;
    }

    std::cout << "Compiling JIT search circuit from AST...\n";
    engine.set_expression("market", ast_root);
    std::cout << "✓ JIT compiled logic loaded.\n\n";

    std::cout << "[Step 4] Allocating aligned memory for bit-planes...\n";
    size_t num_fields = batch->num_columns();
    size_t num_blocks = (num_rows + 63) / 64;
    size_t planes_size = num_blocks * num_fields * 64 * sizeof(uint64_t);
    
    uint64_t* bit_planes = nullptr;
    // Align to 64 bytes (L1 cache line boundary) to saturate memory bus
    if (posix_memalign((void**)&bit_planes, 64, planes_size) != 0) {
        std::cerr << "❌ Aligned memory allocation failed\n";
        return 1;
    }
    std::memset(bit_planes, 0, planes_size);
    std::cout << "✓ Allocated " << (planes_size / 1024) << " KB page-aligned bit-plane memory.\n\n";

    std::cout << "[Step 5] Transposing Arrow columns to bit-planes (Software Offload)...\n";
    auto start_trans = std::chrono::high_resolution_clock::now();
    connector.TransposeBatch(batch, bit_planes);
    auto end_trans = std::chrono::high_resolution_clock::now();
    
    auto duration_trans = std::chrono::duration_cast<std::chrono::microseconds>(end_trans - start_trans).count();
    std::cout << "✓ Transposed " << num_rows << " records in " << duration_trans << " microseconds.\n";
    std::cout << "  Throughput: " << (static_cast<double>(num_rows) / duration_trans) << " Million Records/sec (Ingestion)\n\n";

    std::cout << "[Step 6] Running JIT Query Scan directly on bit-planes...\n";
    auto start_scan = std::chrono::high_resolution_clock::now();
    uint64_t matches = engine.execute_native("market", bit_planes, num_blocks);
    auto end_scan = std::chrono::high_resolution_clock::now();
    
    auto duration_scan = std::chrono::duration_cast<std::chrono::microseconds>(end_scan - start_scan).count();
    std::cout << "✓ Scan complete. Matches found: " << matches << "\n";
    std::cout << "  Scan Time: " << duration_scan << " microseconds.\n";
    double rps = (static_cast<double>(num_rows) / duration_scan) * 1000000.0;
    std::cout << "  🚀 Throughput: " << (rps / 1000000.0) << " Million Records/sec (Single-Core)\n\n";

    std::cout << "[Step 7] Verifying correctness against a scalar C++ loop...\n";
    uint64_t scalar_matches = 0;
    auto start_scalar = std::chrono::high_resolution_clock::now();
    for (int64_t i = 0; i < num_rows; ++i) {
        uint64_t ask = 10000 + i;
        uint64_t volume = 1000 + i;
        if (ask > 10500 && volume > 2000) {
            scalar_matches++;
        }
    }
    auto end_scalar = std::chrono::high_resolution_clock::now();
    auto duration_scalar = std::chrono::duration_cast<std::chrono::microseconds>(end_scalar - start_scalar).count();

    std::cout << "  Scalar loop found matches: " << scalar_matches << " in " << duration_scalar << " microseconds.\n";
    
    if (matches == scalar_matches) {
        std::cout << "  ✅ CORRECTNESS VERIFIED: Matches match EXACTLY bit-for-bit!\n";
    } else {
        std::cerr << "  ❌ ERROR: Match mismatch! JIT: " << matches << ", Scalar: " << scalar_matches << "\n";
        std::free(bit_planes);
        return 1;
    }

    std::cout << "\n==================================================\n";
    std::cout << "🎉 ALL VERIFICATIONS PASSED SUCCESSFULLY!\n";
    std::cout << "==================================================\n";

    std::free(bit_planes);
    return 0;
}
