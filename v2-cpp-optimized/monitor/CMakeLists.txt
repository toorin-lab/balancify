add_executable(balancify_monitor
    src/main.cpp
)

target_include_directories(balancify_monitor PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(balancify_monitor PRIVATE Boost::system Boost::thread Boost::json Threads::Threads)

