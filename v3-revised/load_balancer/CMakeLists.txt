find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED IMPORTED_TARGET libdpdk)
find_package(Boost 1.74 REQUIRED)

add_executable(balancify_lb
    src/main.cpp
    src/config.cpp
    src/runtime.cpp
    src/dip_pool.cpp
    src/hash_path.cpp
    src/conn_table.cpp
    src/counting_bloom.cpp
    src/load_monitor.cpp
    src/worker.cpp
    src/dataplane.cpp
    src/coordinator.cpp
    src/cache_allocation.cpp
    src/stats.cpp
    src/control_api.cpp
)

target_include_directories(balancify_lb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(balancify_lb PRIVATE balancify_common PkgConfig::DPDK Boost::boost Threads::Threads)
# DPDK drivers register themselves from constructors; keep them linked in.
target_link_options(balancify_lb PRIVATE -Wl,--no-as-needed)
target_compile_definitions(balancify_lb PRIVATE ALLOW_EXPERIMENTAL_API)

if(BALANCIFY_WITH_ZOOKEEPER)
    find_path(ZOOKEEPER_INCLUDE_DIR zookeeper/zookeeper.h)
    find_library(ZOOKEEPER_LIBRARY NAMES zookeeper_mt)
    if(NOT ZOOKEEPER_INCLUDE_DIR OR NOT ZOOKEEPER_LIBRARY)
        message(FATAL_ERROR "ZooKeeper C client (zookeeper_mt) not found; run scripts/install_deps.sh "
                            "or configure with -DBALANCIFY_WITH_ZOOKEEPER=OFF")
    endif()
    target_include_directories(balancify_lb PRIVATE ${ZOOKEEPER_INCLUDE_DIR})
    target_link_libraries(balancify_lb PRIVATE ${ZOOKEEPER_LIBRARY})
    target_compile_definitions(balancify_lb PRIVATE BALANCIFY_HAS_ZOOKEEPER THREADED)
endif()

if(BALANCIFY_WITH_PQOS)
    find_path(PQOS_INCLUDE_DIR pqos.h)
    find_library(PQOS_LIBRARY NAMES pqos)
    if(NOT PQOS_INCLUDE_DIR OR NOT PQOS_LIBRARY)
        message(FATAL_ERROR "libpqos (intel-cmt-cat) not found; run scripts/install_deps.sh "
                            "or configure with -DBALANCIFY_WITH_PQOS=OFF")
    endif()
    target_include_directories(balancify_lb PRIVATE ${PQOS_INCLUDE_DIR})
    target_link_libraries(balancify_lb PRIVATE ${PQOS_LIBRARY})
    target_compile_definitions(balancify_lb PRIVATE BALANCIFY_HAS_PQOS)
endif()

install(TARGETS balancify_lb RUNTIME DESTINATION bin)
