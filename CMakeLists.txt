cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(Sherman)

option(BUILD_TEST "Whether or not build the tests" ON)
option(BUILD_BENCH "Whether or not build the benchmark" ON)
option(SANITIZE "Turn on sanitization" OFF)

# set (CMAKE_C_COMPILER "mpicxx")
# set (CMAKE_CXX_COMPILER ${CMAKE_C_COMPILER})

# Compiler options
set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare -Wno-address-of-packed-member")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17 -g3 -Wall -Wextra -Werror -mavx")

# turn on these later
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wcast-align -Wcast-qual -Wctor-dtor-privacy -Wdisabled-optimization -Wformat=2 -Winit-self -Wmissing-declarations -Wmissing-include-dirs -Woverloaded-virtual -Wredundant-decls -Wshadow -Wsign-promo -Wstrict-overflow=5 -Wswitch-default -Wundef -Werror -Wno-unused  -Wno-format-nonliteral")

# for Magick++
add_definitions(-DMAGICKCORE_QUANTUM_DEPTH=16)
add_definitions(-DMAGICKCORE_HDRI_ENABLE=0)
find_package(ImageMagick COMPONENTS Magick++)

# use SYSTEM GLAD to disable warning from libraries
include_directories(SYSTEM GLAD ${ImageMagick_INCLUDE_DIRS})
message(STATUS ${ImageMagick_INCLUDE_DIRS})
link_libraries(${ImageMagick_LIBRARIES})

# enable profiling
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pg ")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg ")
add_subdirectory(thirdparty/DataFrame)

# disable boost warning
add_definitions(-DBOOST_COROUTINES_NO_DEPRECATION_WARNING)

set(Boost_USE_STATIC_LIBS ON)
find_package(
    Boost
    1.59
    REQUIRED
    coroutine
    system
    thread
)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

# message(STATUS ${CMAKE_MODULE_PATH})
find_package(
    glog REQUIRED
)

find_package(gflags REQUIRED)

# find_package(benchmark REQUIRED)
# link_libraries(benchmark::benchmark)

# Link Options
set(LINKS_FLAGS "-lpthread -libverbs -lmemcached -rdynamic -lstdc++fs")
find_library(CITYHASH_LIB NAMES libcityhash.a REQUIRED)

link_libraries(${CITYHASH_LIB})

link_libraries(unwind)

# boost coroutine does not work well with boost::coroutine and boost::coroutine2
# if (CMAKE_BUILD_TYPE STREQUAL "Debug")
# set(ASAN_OPTIONS "fast_unwind_on_fatal=1")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")
# add_definitions(-DBOOST_USE_ASAN)

# endif()
# set(ASAN_OPTIONS "fast_unwind_on_fatal=1")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")
set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})
include_directories(${GLOG_INCLUDE_DIRS})

# Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

# Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(sherman STATIC ${COMMON_FILE})
link_libraries(sherman)
link_libraries(${GLOG_LIBRARIES})
link_libraries(${GFLAGS_LIBRARIES})

# bench
if(${BUILD_BENCH})
    file(GLOB BENCHMARK_SRC ${PROJECT_SOURCE_DIR}/bench/*.cpp)

    foreach(BENCHMARK ${BENCHMARK_SRC})
        get_filename_component(BENCHMARK_NAME ${BENCHMARK} NAME_WE)
        add_executable(${BENCHMARK_NAME} ${BENCHMARK})
        target_link_libraries(${BENCHMARK_NAME} PRIVATE ${LINKS_FLAGS} ${Boost_LIBRARIES} sherman DataFrame::DataFrame)
        target_include_directories(${BENCHMARK_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
    endforeach()
endif()

# test
if(${BUILD_TEST})
    file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)

    foreach(TEST ${TEST_SRC})
        get_filename_component(TEST_NAME ${TEST} NAME_WE)
        add_executable(${TEST_NAME} ${TEST})
        target_link_libraries(${TEST_NAME} PRIVATE ${LINKS_FLAGS} ${Boost_LIBRARIES} sherman DataFrame::DataFrame)
        target_include_directories(${TEST_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
    endforeach()
endif()
