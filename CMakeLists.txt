cmake_minimum_required(VERSION 3.0)
project(database LANGUAGES CXX)

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH};${CMAKE_CURRENT_SOURCE_DIR})
find_package(RocksDB REQUIRED)
find_package(Catch2 REQUIRED)
find_package(Protobuf REQUIRED)
find_package(testUtils REQUIRED)
enable_testing()

addCatchTest(rocksdbTester rocksdbTester.cpp)
target_link_libraries(rocksdbTester PRIVATE RocksDB::rocksdb)
set_target_properties(rocksdbTester PROPERTIES CXX_STANDARD 17)

addCatchTest(protobufDynMsgTester protobufDynMsg.cpp)
target_link_libraries(protobufDynMsgTester PRIVATE protobuf::libprotobuf)


add_executable(example example.cpp desc.proto)
target_link_libraries(example RocksDB::rocksdb protobuf::libprotobuf)
set_target_properties(example PROPERTIES CXX_STANDARD 17)
target_include_directories(example PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET example)