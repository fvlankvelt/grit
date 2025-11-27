cmake_minimum_required(VERSION 3.28)

project(Grit)

find_package(RocksDB REQUIRED)
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)

# Set source and output directories
set(PROTO_FILE ${CMAKE_SOURCE_DIR}/storage.proto)

# Command to generate C++ files from the proto file
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/storage.pb.cc ${CMAKE_SOURCE_DIR}/storage.pb.h
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_SOURCE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}
            ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating Protobuf C++ files from ${PROTO_FILE}"
    VERBATIM
)

# Custom target for generating proto files
add_custom_target(generate_storage
    DEPENDS ${CMAKE_SOURCE_DIR}/storage.pb.cc ${CMAKE_SOURCE_DIR}/storage.pb.h
    COMMENT "Running generate_storage target"
)

# DynamicGraph
add_executable(Grit)
target_sources(Grit PRIVATE main.cc ${CMAKE_SOURCE_DIR}/storage.pb.cc)
target_link_libraries(Grit PRIVATE 
  RocksDB::rocksdb-shared
  protobuf::libprotobuf
  absl::log
  absl::check
)
set_property(TARGET Grit PROPERTY CXX_STANDARD 20)

add_dependencies(Grit generate_storage)

