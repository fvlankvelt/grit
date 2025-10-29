cmake_minimum_required(VERSION 3.28)

project(DynamicGraph)

find_package(RocksDB REQUIRED)

# DynamicGraph
add_executable(DynamicGraph)
target_sources(DynamicGraph PRIVATE main.cc)
target_link_libraries(DynamicGraph PRIVATE RocksDB::rocksdb-shared)

