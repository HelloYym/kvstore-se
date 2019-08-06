cmake_minimum_required(VERSION 2.8)
project(kv_service)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -DELPP_NO_DEFAULT_LOG_FILE -DELPP_THREAD_SAFE")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
    ${SRC_BASE}/../include
    ${SRC_BASE}/../../third_party
    )


link_directories(
    )

set(SRC_SERVICE
    ${SRC_BASE}/kv_service.cc
    )

add_library(${PROJECT_NAME} SHARED ${SRC_SERVICE})
