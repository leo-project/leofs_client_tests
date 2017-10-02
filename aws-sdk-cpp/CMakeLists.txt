# Same as that for aws-sdk-cpp
cmake_minimum_required (VERSION 2.8.12)
project(leofs_client_tests_aws_sdk_cpp)

# 3.0 or higher is strongly suggested; build settings (target_compile_options/etc...) sometimes do not get propagated properly under certain conditions prior to this version
# Making this a hard requirement is potentially disruptive to existing customers who aren't affected by the bad behavior though, so just warn for now
if(CMAKE_MAJOR_VERSION LESS 3)
    message(WARNING "Building with CMake 3.0 or higher is strongly suggested; current version is ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")
endif()

MACRO(SUBDIRLIST result curdir)
  FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
  SET(dirlist "")
  FOREACH(child ${children})
    IF(IS_DIRECTORY ${curdir}/${child})
      LIST(APPEND dirlist ${child})
    ENDIF()
  ENDFOREACH()
  SET(${result} ${dirlist})
ENDMACRO()

SET(AWS_PREFIX extern/aws-sdk-cpp)
SET(AWS_INCLUDE_DIRECTORIES "")
SET(AWS_LIBRARY_DIRECTORIES "")

SUBDIRLIST(SUBDIRS ${CMAKE_SOURCE_DIR}/${AWS_PREFIX})

FOREACH(subdir ${SUBDIRS})
  SET(AWS_INC_DIR ${CMAKE_SOURCE_DIR}/${AWS_PREFIX}/${subdir}/include)
  SET(AWS_BIN_DIR ${CMAKE_BINARY_DIR}/${AWS_PREFIX}/${subdir})
  IF(IS_DIRECTORY ${AWS_INC_DIR})
      LIST(APPEND AWS_INCLUDE_DIRECTORIES ${AWS_INC_DIR})
      LIST(APPEND AWS_LIBRARY_DIRECTORIES ${AWS_BIN_DIR})
  ENDIF()
ENDFOREACH()
# message(STATUS ${AWS_INCLUDE_DIRECTORIES})
# get_property(inc_dir DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

add_subdirectory(extern/aws-sdk-cpp)

SET(TEST_CXX_FLAGS "-std=c++11")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TEST_CXX_FLAGS}")

include_directories(${AWS_INCLUDE_DIRECTORIES})
include_directories(include)
SET(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${AWS_LIBRARY_DIRECTORIES})

add_executable(LeoFSTest.cpp src/main.cpp)
target_link_libraries(LeoFSTest.cpp aws-cpp-sdk-core)
target_compile_definitions(LeoFSTest.cpp PRIVATE CURRDIR=${CMAKE_BINARY_DIR})
