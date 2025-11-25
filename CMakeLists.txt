cmake_minimum_required(VERSION 3.28)

project(Grit)

find_package(RocksDB REQUIRED)

# DynamicGraph
add_executable(Grit)
target_sources(Grit PRIVATE main.cc)
target_link_libraries(Grit PRIVATE RocksDB::rocksdb-shared)
set_property(TARGET Grit PROPERTY CXX_STANDARD 20)

