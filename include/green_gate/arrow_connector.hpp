#pragma once

#include <arrow/api.h>
#include "apex/AarchGate.hpp"
#include "apex/engine.hpp"

namespace greengate {

class ArrowConnector {
public:
    explicit ArrowConnector(apex::ApexEngine& engine) : engine_(engine) {}

    // Register a schema in AarchGate from an Arrow Schema
    void RegisterSchema(const std::string& schema_name, 
                        const std::shared_ptr<arrow::Schema>& schema);

    // Extract primitive columns from the RecordBatch and transpose them block-by-block.
    // Each block is exactly 64 rows. The destination bit-planes array is assumed to be
    // pre-allocated with size: num_blocks * num_fields * 64 * sizeof(uint64_t) bytes.
    void TransposeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, 
                        uint64_t* out_bit_planes);

private:
    apex::ApexEngine& engine_;
};

} // namespace greengate
