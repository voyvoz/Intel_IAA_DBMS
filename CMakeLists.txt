cmake_minimum_required(VERSION 3.10)
project(BenchmarkApp)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_BUILD_TYPE Release)

# Use QPL's CMake config package (must be installed and discoverable)
find_package(QPL CONFIG REQUIRED)

# Project headers
include_directories(include)

# Gather sources
file(GLOB_RECURSE SRC_FILES src/*.cpp)
file(GLOB BENCH_FILES bench/*.cpp)

message(STATUS "Source files:")
foreach(file ${SRC_FILES})
    message(STATUS "  ${file}")
endforeach()

# One executable per benchmark
foreach(BENCH_FILE ${BENCH_FILES})
    get_filename_component(BENCH_NAME ${BENCH_FILE} NAME_WE)
    add_executable(${BENCH_NAME}_exec ${BENCH_FILE} ${SRC_FILES})
    target_link_libraries(${BENCH_NAME}_exec PRIVATE QPL::qpl)  # assumes QPL exports this target
endforeach()
