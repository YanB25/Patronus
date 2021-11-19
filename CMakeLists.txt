cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(Sherman)

option(BUILD_TEST "Whether or not build the tests" ON)
option(BUILD_BENCH "Whether or not build the benchmark" OFF)
option(SANITIZE "Turn on sanitization" OFF)

set (CMAKE_C_COMPILER "/usr/bin/mpicxx")
set (CMAKE_CXX_COMPILER ${CMAKE_C_COMPILER})

#Compiler options
set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++11 ")

# disable boost warning
add_definitions(-DBOOST_COROUTINES_NO_DEPRECATION_WARNING)

find_package(
    Boost
    1.59
    REQUIRED
    coroutine
    system
)

# Link Options
set(LINKS_FLAGS "-lcityhash -lpthread -libverbs -lmemcached")

if (${SANITIZE})
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})

#Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

#Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(sherman STATIC ${COMMON_FILE})
link_libraries(sherman)

# bench
if (${BUILD_BENCH})
    file(GLOB BENCHMARK_SRC ${PROJECT_SOURCE_DIR}/bench/*.cpp)
    foreach (BENCHMARK ${BENCHMARK_SRC})
        get_filename_component(BENCHMARK_NAME ${BENCHMARK} NAME_WE)
        add_executable(${BENCHMARK_NAME} ${BENCHMARK})
        target_link_libraries(${BENCHMARK_NAME} ${LINKS_FLAGS})
    endforeach()
endif()

# test
if (${BUILD_TEST})
    file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
    foreach (TEST ${TEST_SRC})
        get_filename_component(TEST_NAME ${TEST} NAME_WE)
        add_executable(${TEST_NAME} ${TEST})
        target_link_libraries(${TEST_NAME} PRIVATE ${LINKS_FLAGS} ${Boost_LIBRARIES})
        target_include_directories(${TEST_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
    endforeach()
endif()