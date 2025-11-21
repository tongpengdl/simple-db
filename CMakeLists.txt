cmake_minimum_required(VERSION 3.16)

project(simple_db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(simpledb
  src/pager.cpp
  src/page.cpp
  src/record.cpp
)

target_include_directories(simpledb PUBLIC include)
target_compile_options(simpledb PRIVATE -Wall -Wextra -Wpedantic)

add_executable(simpledb_cli src/main.cpp)
target_link_libraries(simpledb_cli PRIVATE simpledb)

enable_testing()

add_executable(pager_test tests/pager_test.cpp)
target_link_libraries(pager_test PRIVATE simpledb)
add_test(NAME pager_test COMMAND pager_test)
