cmake_minimum_required(VERSION 3.20)

add_executable(balancify_lb
    src/main.cpp
    src/balancify_engine.cpp
    src/control_plane.cpp
)

target_include_directories(balancify_lb
    PRIVATE
        ${Boost_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(balancify_lb
    PRIVATE
        balancify_common
        ${Boost_LIBRARIES}
        Threads::Threads
)

target_compile_definitions(balancify_lb PRIVATE BOOST_BEAST_USE_STD_STRING_VIEW)

