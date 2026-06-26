#include "lex/ecosystem/pg_wire_server.hpp"
#include "lex/distributed/global_metadata.hpp"
#include "lex/lsm/promotion_daemon.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <numeric>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace greengate;

#define ASSERT_TRUE(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed: " << #cond << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::exit(1); \
        } \
    } while (0)

static std::shared_ptr<arrow::RecordBatch> CreateEcosystemBatch(size_t start_idx, size_t num_rows) {
    arrow::StringBuilder str_builder;
    arrow::Int64Builder int_builder;
    
    for (size_t i = 0; i < num_rows; ++i) {
        size_t idx = start_idx + i;
        // Seed strings with distinct values
        std::string s;
        if (idx % 3 == 0) s = "apple";
        else if (idx % 3 == 1) s = "banana";
        else s = "cherry";

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

// Read integer from network buffer
static int32_t ReadInt32(const char*& ptr) {
    int32_t net_val;
    std::memcpy(&net_val, ptr, 4);
    ptr += 4;
    return ntohl(net_val);
}

static int16_t ReadInt16(const char*& ptr) {
    int16_t net_val;
    std::memcpy(&net_val, ptr, 2);
    ptr += 2;
    return ntohs(net_val);
}

void TestEcosystemIntegrations() {
    std::cout << "[Test] Running Ecosystem integrations..." << std::endl;

    // Initialize 4-node cluster coordinator
    const int num_nodes = 4;
    std::vector<std::pair<std::string, int>> node_addresses = {
        {"127.0.0.1", 9005},
        {"127.0.0.1", 9006},
        {"127.0.0.1", 9007},
        {"127.0.0.1", 9008}
    };

    auto coordinator = std::make_shared<DistributedCoordinator>(num_nodes);
    coordinator->InitializeCluster(node_addresses);

    // Register tables globally
    GlobalMetadata::Instance().Clear();
    coordinator->RegisterTable("L", {"c_str", "c_val"}, {13, 0});
    coordinator->RegisterTable("R", {"c_str", "c_val"}, {13, 0});

    // Ingest test batches
    auto left_batch = CreateEcosystemBatch(0, 100);
    auto right_batch = CreateEcosystemBatch(0, 100);
    coordinator->DistributeAndIngest("L", left_batch);
    coordinator->DistributeAndIngest("R", right_batch);

    // Sync global snapshots
    for (int n = 0; n < num_nodes; ++n) {
        GlobalMetadata::Instance().SyncLocalNode(n, "L");
        GlobalMetadata::Instance().SyncLocalNode(n, "R");
    }

    // 1. Test Advanced Aggregations (Single-table via Local Query Planner)
    MetadataRegistry::Instance().RegisterTable("L_local", {"c_str", "c_val"}, {13, 0});
    MetadataRegistry::Instance().InsertBatch("L_local", left_batch);
    auto state_local = MetadataRegistry::Instance().GetTableState("L_local");

    AdaptiveIngester ingester;
    RowGroup sliced_rg = ingester.Ingest(left_batch);

    // Query: c_val > 100
    std::vector<Predicate> preds = {
        {"c_val", PredicateOp::GT, "", 100, false}
    };
    
    QueryRunner runner = QueryPlanner::Plan(preds, sliced_rg, "c_val", AggregationType::AVG);
    QueryResult avg_res = runner.ExecuteQueryResult(*state_local);
    
    // Also verify aggregation directly on bitsliced CBST RowGroup
    QueryResult rg_res = runner.ExecuteQueryResult(sliced_rg);
    
    // Matched values: indices 11 to 99 -> 89 matches. c_val = 110, 120, ..., 990.
    // Sum = (110 + 990) * 89 / 2 = 48950. Average = 48950 / 89 = 550.0.
    // Median of 89 sorted values is the 45th element -> index 11 + 44 = 55 -> val = 550.
    // Distinct counts: c_val has 89 distinct values.
    std::cout << "  [Local Query] Matches: " << avg_res.count << " (Expected: 89)" << std::endl;
    std::cout << "  [Local Query] Sum: " << avg_res.sum << " (Expected: 48950)" << std::endl;
    std::cout << "  [Local Query] Average: " << avg_res.avg << " (Expected: 550.0)" << std::endl;
    std::cout << "  [Local Query] Min: " << avg_res.min_val << " (Expected: 110)" << std::endl;
    std::cout << "  [Local Query] Max: " << avg_res.max_val << " (Expected: 990)" << std::endl;
    std::cout << "  [Local Query] Median: " << avg_res.median_val << " (Expected: 550)" << std::endl;
    std::cout << "  [Local Query] Distinct Count: " << avg_res.distinct_count << " (Expected: 89)" << std::endl;

    ASSERT_TRUE(avg_res.count == 89);
    ASSERT_TRUE(avg_res.sum == 48950);
    ASSERT_TRUE(avg_res.avg == 550.0);
    ASSERT_TRUE(avg_res.min_val == 110);
    ASSERT_TRUE(avg_res.max_val == 990);
    ASSERT_TRUE(avg_res.median_val == 550);
    ASSERT_TRUE(avg_res.distinct_count == 89);

    // Verify bitsliced RowGroup results
    ASSERT_TRUE(rg_res.count == 89);
    ASSERT_TRUE(rg_res.sum == 48950);
    ASSERT_TRUE(rg_res.avg == 550.0);

    // 2. Test Zonal-Aware S3 Express Scheduling
    // Nodes: Node 0 -> us-east-1-az1, ..., Node 3 -> us-east-1-az4.
    // Bucket suffix: "--use1-az4--x-s3" should route to Node 3.
    std::string bucket_name = "greengate--use1-az4--x-s3";
    int scheduled_node = coordinator->ScheduleTask(bucket_name);
    std::cout << "  [Zonal Schedule] Scheduled task for '" << bucket_name << "' on Node " << scheduled_node << " (Expected: 3)" << std::endl;
    ASSERT_TRUE(scheduled_node == 3);

    // 3. Test Auto-Pilot Promotion Access tracking
    PromotionDaemon::ClearColumnAccessCounts();
    for (int i = 0; i < 6; ++i) {
        runner.ExecuteQueryResult(*state_local);
    }
    uint64_t count_str = PromotionDaemon::GetColumnAccessCount("c_val");
    std::cout << "  [Auto-Pilot] Recorded query count for 'c_val': " << count_str << " (Expected: 12)" << std::endl;
    ASSERT_TRUE(count_str == 12);

    // 4. Test PostgreSQL Wire Server Handshake and simple query execution
    int pg_port = 5433;
    PgWireServer pg_server(pg_port, coordinator);
    pg_server.Start();

    // Spawn a simulated PG socket client
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_TRUE(sock != -1);
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(pg_port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    ASSERT_TRUE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) == 0);

    // Send StartupMessage
    // Format: length (4 bytes), version (4 bytes), null-terminated param pairs (user=postgres, database=greengate), extra null byte
    std::vector<char> startup;
    // placeholder for length
    uint32_t dummy_len = 0;
    startup.insert(startup.end(), reinterpret_cast<char*>(&dummy_len), reinterpret_cast<char*>(&dummy_len) + 4);
    uint32_t proto = htonl(196608); // 3.0
    startup.insert(startup.end(), reinterpret_cast<char*>(&proto), reinterpret_cast<char*>(&proto) + 4);
    auto push_str = [&](const std::string& s) {
        startup.insert(startup.end(), s.begin(), s.end());
        startup.push_back('\0');
    };
    push_str("user");
    push_str("postgres");
    push_str("database");
    push_str("greengate");
    startup.push_back('\0');
    
    uint32_t final_len = htonl(startup.size());
    std::memcpy(startup.data(), &final_len, 4);

    ASSERT_TRUE(send(sock, startup.data(), startup.size(), 0) == static_cast<ssize_t>(startup.size()));

    // Read Handshake response: AuthenticationOk, ParameterStatus, ReadyForQuery
    char resp_buf[1024];
    ssize_t bytes_recv = recv(sock, resp_buf, sizeof(resp_buf), 0);
    ASSERT_TRUE(bytes_recv > 0);
    std::cout << "  [PG Handshake] Handshake bytes received: " << bytes_recv << std::endl;

    // Send Query Message ('Q'): select sum(L.c_val) from L join R on L.c_str = R.c_str where L.c_val > 100
    std::string sql = "SELECT SUM(L.c_val) FROM L JOIN R ON L.c_str = R.c_str WHERE L.c_val > 100";
    std::vector<char> q_msg;
    q_msg.push_back('Q');
    uint32_t q_len = htonl(4 + sql.size() + 1);
    q_msg.insert(q_msg.end(), reinterpret_cast<char*>(&q_len), reinterpret_cast<char*>(&q_len) + 4);
    q_msg.insert(q_msg.end(), sql.begin(), sql.end());
    q_msg.push_back('\0');

    ASSERT_TRUE(send(sock, q_msg.data(), q_msg.size(), 0) == static_cast<ssize_t>(q_msg.size()));

    // Read Query response: RowDescription ('T'), DataRow ('D'), CommandComplete ('C'), ReadyForQuery ('Z')
    std::vector<char> q_resp;
    while (true) {
        char chunk[1024];
        ssize_t chunk_bytes = recv(sock, chunk, sizeof(chunk), 0);
        if (chunk_bytes <= 0) break;
        q_resp.insert(q_resp.end(), chunk, chunk + chunk_bytes);
        if (chunk[chunk_bytes - 1] == 'I') { // Idle indicator in ReadyForQuery
            break;
        }
    }
    std::cout << "  [PG Query] Query response bytes received: " << q_resp.size() << std::endl;
    ASSERT_TRUE(!q_resp.empty());

    // Parse DataRow ('D') to check the result value
    // Let's locate the 'D' byte
    const char* ptr = q_resp.data();
    const char* end = ptr + q_resp.size();
    bool found_datarow = false;
    std::string result_val;

    while (ptr < end) {
        char type = *ptr;
        ptr++;
        int32_t msg_size = ReadInt32(ptr);
        (void)msg_size;
        
        if (type == 'D') {
            found_datarow = true;
            int16_t num_cols = ReadInt16(ptr);
            ASSERT_TRUE(num_cols == 1);
            int32_t col_len = ReadInt32(ptr);
            ASSERT_TRUE(col_len > 0);
            result_val.assign(ptr, col_len);
            ptr += col_len;
            break;
        } else {
            ptr += (msg_size - 4);
        }
    }

    std::cout << "  [PG Query] DataRow found: " << std::boolalpha << found_datarow << std::endl;
    std::cout << "  [PG Query] Output Join Aggregation Result: " << result_val << std::endl;
    
    // Let's calculate expected output:
    // Left batch has 100 rows. c_val > 100 leaves indices 11 to 99 (89 rows).
    // All indices match c_str pattern since Left and Right are identical.
    // L.c_val sum for many-to-many join = 1,632,000.
    ASSERT_TRUE(found_datarow);
    ASSERT_TRUE(result_val == "1632000");

    close(sock);
    pg_server.Stop();
    coordinator->ShutdownCluster();

    std::cout << "  -> Ecosystem integrations Passed!" << std::endl;
}

int main() {
    std::cout << "===========================================" << std::endl;
    std::cout << "Running Phase 5 Ecosystem Scaling Tests..." << std::endl;
    std::cout << "===========================================" << std::endl;

    TestEcosystemIntegrations();

    std::cout << "===========================================" << std::endl;
    std::cout << "All Phase 5 Ecosystem Scaling Tests PASSED!" << std::endl;
    std::cout << "===========================================" << std::endl;
    return 0;
}
