cmake_minimum_required(VERSION 3.18)
project(GreenGate VERSION 1.0.0 LANGUAGES CXX)

# Require C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Configure dependencies
find_package(Arrow REQUIRED)
find_package(CURL REQUIRED)

# Force-disable unnecessary AarchGate sub-features to bypass missing JDK/flatc/python
set(APEX_BUILD_TESTS OFF CACHE BOOL "Disable tests" FORCE)
set(APEX_BUILD_BENCHMARKS OFF CACHE BOOL "Disable benchmarks" FORCE)
set(APEX_BUILD_EXAMPLES OFF CACHE BOOL "Disable examples" FORCE)
set(APEX_BUILD_PYTHON OFF CACHE BOOL "Disable Python bindings" FORCE)
set(APEX_BUILD_JAVA OFF CACHE BOOL "Disable Java bindings" FORCE)
set(APEX_BUILD_FLATBUFFERS OFF CACHE BOOL "Disable FlatBuffers" FORCE)
set(IOX_PLATFORM_FEATURE_ACL OFF CACHE BOOL "Disable iceoryx ACL" FORCE)

# Build aarchgate subdirectory
add_subdirectory(aarchgate)

# Include directories
include_directories(
    include
    aarchgate/include
    aarchgate/external/asmjit/src # For asmjit headers if needed
)

# Core sources used by multiple targets
set(CORE_SOURCES
    src/arrow_connector.cpp
    src/sql_parser.cpp
    src/shm_bridge.cpp
)

# Executable targets
add_executable(green_gate_verify ${CORE_SOURCES} src/main.cpp)
target_compile_options(green_gate_verify PRIVATE -O3 -ffast-math -Wall -Wextra)
target_link_libraries(green_gate_verify PRIVATE 
    aarchgate
    Arrow::arrow_shared
)

add_executable(green_gate_daemon 
    ${CORE_SOURCES}
    src/local_cache.cpp
    src/query_daemon.cpp
    src/daemon_main.cpp
)
target_compile_options(green_gate_daemon PRIVATE -O3 -ffast-math -Wall -Wextra)
target_link_libraries(green_gate_daemon PRIVATE 
    aarchgate
    Arrow::arrow_shared
    CURL::libcurl
)

add_executable(green_gate_benchmark
    ${CORE_SOURCES}
    src/s3_writer.cpp
    src/benchmark_client.cpp
)
target_compile_options(green_gate_benchmark PRIVATE -O3 -ffast-math -Wall -Wextra)
target_link_libraries(green_gate_benchmark PRIVATE 
    aarchgate
    Arrow::arrow_shared
    CURL::libcurl
)
