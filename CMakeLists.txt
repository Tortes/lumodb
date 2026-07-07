cmake_minimum_required(VERSION 3.20)

project(tdldb VERSION 0.1.0 LANGUAGES CXX)

option(TDLDB_BUILD_TESTS "Build tdldb tests" ON)
option(TDLDB_BUILD_BENCHMARKS "Build tdldb benchmark executable" ON)

add_library(tdldb
  src/Database.cpp
  src/File.cpp
  src/Hash.cpp
)

target_include_directories(tdldb
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_features(tdldb PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(tdldb PRIVATE /W4)
else()
  target_compile_options(tdldb PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(TDLDB_BUILD_TESTS)
  enable_testing()
  add_executable(tdldb_tests tests/TestMain.cpp)
  target_link_libraries(tdldb_tests PRIVATE tdldb)
  target_compile_features(tdldb_tests PRIVATE cxx_std_20)
  add_test(NAME tdldb_tests COMMAND tdldb_tests)
endif()

if(TDLDB_BUILD_BENCHMARKS)
  add_executable(tdldb_bench bench/Benchmark.cpp)
  target_link_libraries(tdldb_bench PRIVATE tdldb)
  target_compile_features(tdldb_bench PRIVATE cxx_std_20)
endif()
