add_executable(balancify_client
    src/main.cpp
)

target_include_directories(balancify_client PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(balancify_client PRIVATE Boost::system Boost::thread Boost::json Threads::Threads)

