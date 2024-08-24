cmake_minimum_required (VERSION 3.13)

project (sort-seastar)

set (CMAKE_CXX_STANDARD 23)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (TARGET ssort)

if(NOT DEFINED CMAKE_PREFIX_PATH)
    if (NOT DEFINED ENV{CMAKE_PREFIX_PATH})
        message (FATAL_ERROR "Missing CMAKE_PREFIX_PATH")
    endif()
    set(CMAKE_PREFIX_PATH $ENV{CMAKE_PREFIX_PATH})
endif()
if(NOT DEFINED CMAKE_MODULE_PATH)
    if (NOT DEFINED ENV{CMAKE_MODULE_PATH})
        message (FATAL_ERROR "Missing CMAKE_MODULE_PATH")
    endif()
    set(CMAKE_MODULE_PATH $ENV{CMAKE_MODULE_PATH})
endif()
find_package (Seastar REQUIRED)

add_executable (${TARGET} "ssort.cc")

target_compile_options(${TARGET} PRIVATE -Wall -O2 -g)

option (ENABLE_UBSAN OFF)
set (UBSAN_OPTIONS -fsanitize=undefined)
if (NOT ENABLE_UBSAN)
    message (STATUS "Not using UBSan")
else()
    message (STATUS "Using UBSan")
    target_compile_options(${TARGET} PRIVATE ${UBSAN_OPTIONS})
    target_link_options(${TARGET} PRIVATE ${UBSAN_OPTIONS})
endif()

target_link_libraries (${TARGET} PRIVATE Seastar::seastar)
