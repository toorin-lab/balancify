add_executable(balancify_server
    src/main.cpp
)

target_include_directories(balancify_server PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(balancify_server PRIVATE Boost::system Boost::filesystem Boost::thread Boost::json Threads::Threads)

