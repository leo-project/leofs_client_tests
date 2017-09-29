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
SUBDIRLIST(SUBDIRS ${CMAKE_SOURCE_DIR}/${AWS_PREFIX})
FOREACH(subdir ${SUBDIRS})
  SET(AWS_DIR ${CMAKE_SOURCE_DIR}/${AWS_PREFIX}/${subdir}/include)
  IF(IS_DIRECTORY ${AWS_DIR})
    INCLUDE_DIRECTORIES(SYSTEM ${AWS_DIR})
    message(STATUS ${AWS_DIR})
  ENDIF()
ENDFOREACH()

# get_property(inc_dir DIRECTORY PROPERTY INCLUDE_DIRECTORIES)

#set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build/bin)

#set(CMAKE_INSTALL_BINDIR ${CMAKE_SOURCE_DIR}/build/bin)
#set(CMAKE_INSTALL_INCLUDEDIR ${CMAKE_SOURCE_DIR}/build/include)
#set(CMAKE_INSTALL_LIBDIR ${CMAKE_SOURCE_DIR}/build/lib)

#set(AWS_CPP_SDK_INCLUDE_DIRS /path/to/include/aws)
#set(AWS_CPP_SDK_CORE_LIB "-l:/path/to/libaws-cpp-sdk-core.so"
#set(AWS_CPP_SDK_S3_LIB "-l:/path/to/libaws-cpp-sdk-s3.so"

add_subdirectory(extern/aws-sdk-cpp)

#add_executable(leofs-client-test-cpp src/main.cpp)
#target_link_libraries(leofs-client-test-cpp aws)
