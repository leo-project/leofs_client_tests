# Same as that for aws-sdk-cpp
cmake_minimum_required (VERSION 2.8.12)
project(leofs_client_tests_aws_sdk_cpp)

# 3.0 or higher is strongly suggested; build settings (target_compile_options/etc...) sometimes do not get propagated properly under certain conditions prior to this version
# Making this a hard requirement is potentially disruptive to existing customers who aren't affected by the bad behavior though, so just warn for now
if(CMAKE_MAJOR_VERSION LESS 3)
    message(WARNING "Building with CMake 3.0 or higher is strongly suggested; current version is ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")
endif()

include(aws-sdk-cpp.cmake)

SET(TEST_CXX_FLAGS "-std=c++11")
# SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TEST_CXX_FLAGS}")

include_directories(${AWS_INCLUDE_DIRECTORIES})
include_directories(include)
SET(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${AWS_LIBRARY_DIRECTORIES})

add_executable(LeoFSTest.cpp src/main.cpp)
target_link_libraries(LeoFSTest.cpp aws-cpp-sdk-core aws-cpp-sdk-s3)
target_compile_definitions(LeoFSTest.cpp PRIVATE CURR_DIR="${CMAKE_BINARY_DIR}/")
set_target_properties(LeoFSTest.cpp PROPERTIES COMPILE_FLAGS ${TEST_CXX_FLAGS})
