#pragma once
#include <arrow/api.h>
#include "lex/storage/row_group.hpp"
#include <string_view>

namespace greengate {

uint64_t GenerateKimSignature(std::string_view str);

class AdaptiveIngester {
public:
    AdaptiveIngester() = default;
    ~AdaptiveIngester() = default;

    // Ingests an Arrow RecordBatch, transposes columns into CBST tiles, 
    // and populates the RowGroup structure.
    RowGroup Ingest(const std::shared_ptr<arrow::RecordBatch>& batch);
};

} // namespace greengate
