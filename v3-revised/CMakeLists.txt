cmake_minimum_required(VERSION 3.20)
project(balancify_v3 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(BALANCIFY_BUILD_LB "Build the DPDK load balancer" ON)
option(BALANCIFY_BUILD_AGENT "Build the DIP monitoring agent" ON)
option(BALANCIFY_BUILD_TESTBED "Build the testbed backend and workload generator" ON)
option(BALANCIFY_WITH_ZOOKEEPER "Share overrides and the bucket table through ZooKeeper" ON)
option(BALANCIFY_WITH_PQOS "Reserve an LLC partition with Intel CAT (libpqos)" ON)
set(BALANCIFY_MARCH "native" CACHE STRING "Value passed to -march")

add_compile_options(-Wall -Wextra -Wno-missing-field-initializers)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -march=${BALANCIFY_MARCH})
endif()

find_package(Threads REQUIRED)

add_library(balancify_common INTERFACE)
target_include_directories(balancify_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/common/include)

if(BALANCIFY_BUILD_LB)
    add_subdirectory(load_balancer)
endif()

if(BALANCIFY_BUILD_AGENT)
    add_subdirectory(agent)
endif()

if(BALANCIFY_BUILD_TESTBED)
    add_subdirectory(testbed)
endif()
