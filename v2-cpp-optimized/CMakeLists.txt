cmake_minimum_required(VERSION 3.20)
project(cpp_balancify LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_LOAD_BALANCER "Build the Balancify load balancer" ON)
option(BUILD_CPP_SERVER "Build the Balancify server" ON)
option(BUILD_CPP_MONITOR "Build the Balancify monitor" ON)
option(BUILD_CPP_CLIENT "Build the Balancify client" ON)

find_package(Threads REQUIRED)
find_package(Boost 1.74 REQUIRED COMPONENTS system filesystem thread json)
find_package(OpenSSL REQUIRED)

add_library(balancify_common INTERFACE)
target_include_directories(balancify_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/common/include)
target_link_libraries(balancify_common INTERFACE Boost::boost Threads::Threads OpenSSL::Crypto)

if(BUILD_LOAD_BALANCER)
    add_subdirectory(load_balancer)
endif()

if(BUILD_CPP_SERVER)
    add_subdirectory(server)
endif()

if(BUILD_CPP_MONITOR)
    add_subdirectory(monitor)
endif()

if(BUILD_CPP_CLIENT)
    add_subdirectory(client)
endif()

