cmake_minimum_required(VERSION 3.16)

project(simple_db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(simpledb
  src/disk_manager.cpp
  src/buffer_pool_manager.cpp
  src/page.cpp
  src/record.cpp
)

target_include_directories(simpledb PUBLIC include)
target_compile_options(simpledb PRIVATE -Wall -Wextra -Wpedantic)

add_executable(simpledb_cli src/main.cpp)
target_link_libraries(simpledb_cli PRIVATE simpledb)

enable_testing()

add_executable(buffer_pool_manager_test tests/buffer_pool_manager_test.cpp)
target_link_libraries(buffer_pool_manager_test PRIVATE simpledb)
add_test(NAME buffer_pool_manager_test COMMAND buffer_pool_manager_test)
