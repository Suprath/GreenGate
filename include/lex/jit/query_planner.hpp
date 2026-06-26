#pragma once
#include "lex/storage/row_group.hpp"
#include "lex/jit/micro_op_compiler.hpp"
#include <string>
#include <vector>

namespace greengate {

enum class PredicateOp {
    EQ,
    LIKE_PREFIX,
    LIKE_CONTAINS,
    GT,
    LT
};

struct Predicate {
    std::string column_name;
    PredicateOp op;
    std::string value;            // For string predicates
    uint64_t numeric_value = 0;   // For numeric predicates
    bool is_string = false;
};

struct TableState;

struct QueryRunner {
    BlockScanFunc scan_func;
    std::vector<Predicate> string_predicates;
    std::vector<Predicate> all_predicates;  // Store all query predicates (string + numeric)
    std::vector<size_t> string_col_indices; // Maps each string_predicates[i] to its index among string columns
    std::vector<std::string> clean_string_patterns; // Pre-processed query patterns (e.g. without '%')
    int agg_col_idx = -1;                   // Physical index of numeric column to materialize/transpose
    int max_agg_bit = 0;                    // Pre-calculated maximum active bit plane of the aggregation column
    std::string agg_col_name;               // Name of the aggregation column

    uint64_t Execute(const RowGroup& rg, uint64_t& out_agg_sum);
    uint64_t Execute(const TableState& state, uint64_t& out_agg_sum);
};

class QueryPlanner {
public:
    static QueryRunner Plan(const std::vector<Predicate>& predicates, 
                            const RowGroup& rg, 
                            const std::string& agg_column_name = "");
};

} // namespace greengate
