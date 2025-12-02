cmake_minimum_required(VERSION 3.28)

project(Grit)

find_package(RocksDB REQUIRED)
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)

# Set source and output directories
set(STORAGE_DEF ${CMAKE_SOURCE_DIR}/storage.proto)
set(API_DEF ${CMAKE_SOURCE_DIR}/api.proto)

# Command to generate C++ files from the proto file
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/storage.pb.cc ${CMAKE_SOURCE_DIR}/storage.pb.h
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_SOURCE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}
            ${STORAGE_DEF}
    DEPENDS ${STORAGE_DEF}
    COMMENT "Generating Protobuf C++ files from ${STORAGE_DEF}"
    VERBATIM
)

# Command to generate C++ files from the proto file
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/api.pb.cc ${CMAKE_SOURCE_DIR}/api.pb.h
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_SOURCE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}
            ${API_DEF}
    DEPENDS ${API_DEF}
    COMMENT "Generating Protobuf C++ files from ${API_DEF}"
    VERBATIM
)

# Custom target for generating proto files
add_custom_target(generate_storage
    DEPENDS ${CMAKE_SOURCE_DIR}/storage.pb.cc ${CMAKE_SOURCE_DIR}/storage.pb.h
    COMMENT "Running generate_storage target"
)

# Add NuRaft subdirectory
add_subdirectory(nuraft)

# Grit
add_executable(Grit)
target_sources(Grit PRIVATE
  main.cc
  ${CMAKE_SOURCE_DIR}/storage.pb.cc
  nuraft/examples/in_memory_log_store.cxx
  nuraft/examples/logger.cc
)
target_include_directories(Grit PRIVATE
  nuraft/include/libnuraft
  nuraft/examples
)
target_link_libraries(Grit PRIVATE 
  RocksDB::rocksdb-shared
  protobuf::libprotobuf
  absl::log
  absl::check
  NuRaft::static_lib
)
set_property(TARGET Grit PROPERTY CXX_STANDARD 20)

add_dependencies(Grit generate_storage)

