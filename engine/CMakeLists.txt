cmake_minimum_required(VERSION 3.25)
project(clearline_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
# No -ffast-math (the original used it): replay depends on exact, repeatable
# arithmetic. -O2 rather than -march=native so the image runs on any CPU of
# the target architecture.
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED IMPORTED_TARGET grpc++ protobuf)
find_package(GTest REQUIRED)
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# Generate C++ from proto/matcher/v1/matcher.proto.
set(PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
set(PROTO_FILE ${PROTO_ROOT}/matcher/v1/matcher.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
set(GEN_SRCS
  ${GEN_DIR}/matcher/v1/matcher.pb.cc
  ${GEN_DIR}/matcher/v1/matcher.grpc.pb.cc)
file(MAKE_DIRECTORY ${GEN_DIR})
add_custom_command(
  OUTPUT ${GEN_SRCS}
         ${GEN_DIR}/matcher/v1/matcher.pb.h
         ${GEN_DIR}/matcher/v1/matcher.grpc.pb.h
  COMMAND ${PROTOC} -I ${PROTO_ROOT}
          --cpp_out=${GEN_DIR}
          --grpc_out=${GEN_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE})

# Order book (copied from LowLatencyBuffer, see README.md).
add_library(llb STATIC src/order_book.cpp src/slab_allocator.cpp)
target_include_directories(llb PUBLIC include)

add_library(matcher_core STATIC server/book.cpp server/matcher_service.cpp ${GEN_SRCS})
target_include_directories(matcher_core PUBLIC server ${GEN_DIR})
target_link_libraries(matcher_core PUBLIC llb PkgConfig::GRPC Threads::Threads)

add_executable(matcher-server server/main.cpp)
target_link_libraries(matcher-server PRIVATE matcher_core)

enable_testing()
add_executable(engine_tests tests/book_test.cpp tests/matcher_service_test.cpp)
target_link_libraries(engine_tests PRIVATE matcher_core GTest::gtest GTest::gtest_main)
add_test(NAME engine_tests COMMAND engine_tests)
