function(unit_test)
    set(options SKIP)
    set(oneValueArgs NAME TIMEOUT)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(PARSE_ARGV 0 arg
            "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )

    add_executable(${arg_NAME}
            ${arg_SOURCES}
            ${CMAKE_SOURCE_DIR}/generated/storage.pb.cc
            ${CMAKE_SOURCE_DIR}/generated/api.pb.cc
            ${CMAKE_SOURCE_DIR}/generated/api.grpc.pb.cc
            ${CMAKE_SOURCE_DIR}/nuraft/examples/in_memory_log_store.cxx
            ${CMAKE_SOURCE_DIR}/nuraft/examples/logger.cc
    )
    target_include_directories(${arg_NAME} PRIVATE
            ${CMAKE_SOURCE_DIR}/nuraft/include/libnuraft
            ${CMAKE_SOURCE_DIR}/nuraft/examples
            ${CMAKE_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/generated
    )
    target_link_libraries(${arg_NAME} PRIVATE
            RocksDB::rocksdb-shared
            protobuf::libprotobuf
            absl::log
            absl::check
            NuRaft::static_lib
            gRPC::grpc
            gRPC::grpc++
            gRPC::grpc++_reflection
    )
endfunction()

unit_test(NAME basic_test
        SOURCES
        basic_test.cc
)

unit_test(NAME transaction_test
        SOURCES
        transaction_test.cc
)

unit_test(NAME storage_test
        SOURCES
        storage_test.cc
)

unit_test(NAME test_performance
        SOURCES
        performance_test.cc
)
