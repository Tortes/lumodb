cmake_minimum_required(VERSION 3.20)

project(LumoDB VERSION 0.1.0 LANGUAGES CXX)

option(LUMODB_BUILD_TESTS "Build LumoDB tests" ON)
option(LUMODB_BUILD_BENCHMARKS "Build LumoDB benchmark executable" ON)

add_library(LumoDB
  src/lumodb/database.cpp
  src/lumodb/file.cpp
  src/lumodb/hash.cpp
)

target_include_directories(LumoDB
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(LumoDB PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(LumoDB PRIVATE /W4)
else()
  target_compile_options(LumoDB PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(LUMODB_BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    set(LUMODB_GTEST_HINTS /opt/homebrew /usr/local)
    if(DEFINED ENV{CONDA_PREFIX})
      list(APPEND LUMODB_GTEST_HINTS
        "$ENV{CONDA_PREFIX}"
        "$ENV{CONDA_PREFIX}/.."
        "$ENV{CONDA_PREFIX}/../.."
      )
    endif()
    find_package(GTest QUIET HINTS ${LUMODB_GTEST_HINTS})
  endif()
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
    )
    FetchContent_MakeAvailable(googletest)
  endif()

  include(GoogleTest)

  function(lumodb_configure_gtest target_name)
    target_link_libraries(${target_name} PRIVATE LumoDB GTest::gtest_main)
    target_compile_features(${target_name} PRIVATE cxx_std_20)
    foreach(GTEST_TARGET GTest::gtest_main GTest::gtest)
      foreach(GTEST_LOCATION_PROPERTY
          IMPORTED_LOCATION
          IMPORTED_LOCATION_RELEASE
          IMPORTED_LOCATION_RELWITHDEBINFO
          IMPORTED_LOCATION_MINSIZEREL
          IMPORTED_LOCATION_DEBUG)
        get_target_property(GTEST_LOCATION ${GTEST_TARGET} ${GTEST_LOCATION_PROPERTY})
        if(GTEST_LOCATION)
          get_filename_component(GTEST_LIBRARY_DIR "${GTEST_LOCATION}" DIRECTORY)
          set_property(TARGET ${target_name} APPEND PROPERTY BUILD_RPATH
            "${GTEST_LIBRARY_DIR}"
          )
        endif()
      endforeach()
    endforeach()
    gtest_discover_tests(${target_name} DISCOVERY_MODE PRE_TEST)
  endfunction()

  add_executable(lumodb_tests
    tests/kv_test.cpp
    tests/parallel_row_test.cpp
    tests/rebuild_test.cpp
    tests/row_storage_test.cpp
    tests/struct_test.cpp
  )
  lumodb_configure_gtest(lumodb_tests)

  add_executable(lumodb_system_tests
    tests/system_test.cpp
  )
  lumodb_configure_gtest(lumodb_system_tests)
endif()

if(LUMODB_BUILD_BENCHMARKS)
  add_executable(lumodb_bench bench/benchmark.cpp)
  target_link_libraries(lumodb_bench PRIVATE LumoDB)
  target_compile_features(lumodb_bench PRIVATE cxx_std_20)
endif()
