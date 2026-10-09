find_package(Boost 1.74 REQUIRED)

add_executable(balancify_backend backend/main.cpp)
target_link_libraries(balancify_backend PRIVATE Boost::boost Threads::Threads)

add_executable(balancify_workload workload/main.cpp)
target_link_libraries(balancify_workload PRIVATE Boost::boost Threads::Threads)

install(TARGETS balancify_backend balancify_workload RUNTIME DESTINATION bin)
