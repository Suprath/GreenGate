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

enum class AggregationType {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    MEDIAN,
    DISTINCT
};

struct Predicate {
    std::string column_name;
    PredicateOp op;
    std::string value;            // For string predicates
    uint64_t numeric_value = 0;   // For numeric predicates
    bool is_string = false;
};

struct QueryResult {
    uint64_t count = 0;
    int64_t sum = 0;
    double avg = 0.0;
    int64_t min_val = 0;
    int64_t max_val = 0;
    int64_t median_val = 0;
    uint64_t distinct_count = 0;
    std::vector<std::string> distinct_strings;
    std::vector<int64_t> distinct_numerics;
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
    AggregationType agg_type = AggregationType::COUNT;

    uint64_t Execute(const RowGroup& rg, uint64_t& out_agg_sum);
    uint64_t Execute(const TableState& state, uint64_t& out_agg_sum);

    QueryResult ExecuteQueryResult(const RowGroup& rg);
    QueryResult ExecuteQueryResult(const TableState& state);
};

class QueryPlanner {
public:
    static QueryRunner Plan(const std::vector<Predicate>& predicates, 
                            const RowGroup& rg, 
                            const std::string& agg_column_name = "",
                            AggregationType agg_type = AggregationType::COUNT);
};

} // namespace greengate
