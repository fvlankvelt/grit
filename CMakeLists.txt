cmake_minimum_required(VERSION 3.28)

project(Grit)

find_package(RocksDB REQUIRED)
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)
find_package(OpenSSL REQUIRED)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# Set source and output directories
set(STORAGE_DEF ${CMAKE_SOURCE_DIR}/src/storage.proto)
set(API_DEF ${CMAKE_SOURCE_DIR}/api/api.proto)

# Command to generate C++ files from the proto file
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/generated/storage.pb.cc ${CMAKE_SOURCE_DIR}/generated/storage.pb.h
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_SOURCE_DIR}/generated
            --proto_path=${CMAKE_SOURCE_DIR}/src
            ${STORAGE_DEF}
    DEPENDS ${STORAGE_DEF}
    COMMENT "Generating Protobuf C++ files from ${STORAGE_DEF}"
    VERBATIM
)

# Command to generate C++ files from the proto file
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/generated/api.pb.cc ${CMAKE_SOURCE_DIR}/generated/api.pb.h
           ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.cc ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.h
    COMMAND ${PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_SOURCE_DIR}/generated
            --grpc_out=${CMAKE_SOURCE_DIR}/generated
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
            --proto_path=${CMAKE_SOURCE_DIR}/api
            ${API_DEF}
    DEPENDS ${API_DEF}
    COMMENT "Generating Protobuf C++ files from ${API_DEF}"
    VERBATIM
)

# Custom target for generating proto files
add_custom_target(generate_serialization
    DEPENDS ${CMAKE_SOURCE_DIR}/generated/storage.pb.cc ${CMAKE_SOURCE_DIR}/generated/storage.pb.h
            ${CMAKE_SOURCE_DIR}/generated/api.pb.cc ${CMAKE_SOURCE_DIR}/generated/api.pb.h
            ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.cc ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.h
    COMMENT "Generating serialization code"
)

# Add NuRaft subdirectory
add_subdirectory(nuraft)

# Grit
add_executable(Grit
        src/graph.h)
target_sources(Grit PRIVATE
  src/main.cc
  ${CMAKE_SOURCE_DIR}/generated/storage.pb.cc
  ${CMAKE_SOURCE_DIR}/generated/api.pb.cc
  ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.cc
  nuraft/examples/in_memory_log_store.cxx
  nuraft/examples/logger.cc
)
target_include_directories(Grit PRIVATE
  nuraft/include/libnuraft
  nuraft/examples
  src
  generated
)
target_link_libraries(Grit PRIVATE 
  RocksDB::rocksdb-shared
  protobuf::libprotobuf
  absl::log
  absl::check
  NuRaft::static_lib
  gRPC::grpc
  gRPC::grpc++
  gRPC::grpc++_reflection
)
set_property(TARGET Grit PROPERTY CXX_STANDARD 20)

add_dependencies(Grit generate_serialization)

