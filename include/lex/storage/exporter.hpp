#pragma once
#include "lex/storage/row_group.hpp"
#include <vector>
#include <string>

namespace greengate {

class Exporter {
public:
    // Serializes the RowGroup into a byte stream in the interleaved format.
    static std::vector<uint8_t> Serialize(const RowGroup& rg);

    // Deserializes the RowGroup from a byte stream.
    static RowGroup Deserialize(const std::vector<uint8_t>& data);

    // Helper to write a RowGroup directly to a file.
    static void ExportToFile(const RowGroup& rg, const std::string& filepath);

    // Helper to read a RowGroup directly from a file.
    static RowGroup ImportFromFile(const std::string& filepath);
};

} // namespace greengate
