add_executable(balancify_agent src/main.cpp)
target_link_libraries(balancify_agent PRIVATE balancify_common Threads::Threads)
install(TARGETS balancify_agent RUNTIME DESTINATION bin)
