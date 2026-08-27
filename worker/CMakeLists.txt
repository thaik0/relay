cmake_minimum_required(VERSION 3.20)

project(relay_worker LANGUAGES CXX)

find_package(PostgreSQL REQUIRED)

add_executable(relay-worker src/main.cpp)
target_compile_features(relay-worker PRIVATE cxx_std_20)
target_link_libraries(relay-worker PRIVATE PostgreSQL::PostgreSQL)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(relay-worker PRIVATE -Wall -Wextra -Wpedantic)
endif()
