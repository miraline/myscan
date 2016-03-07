cmake_minimum_required (VERSION 2.8)
project (Simple-Web-Server)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/..)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -Wall")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC version >=4.8 required.")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang version >=3.3 required.")
    endif()
elseif (MSVC14) #TODO: What about other MSVC versions?
else()
    message(WARNING "Your compiler (${CMAKE_CXX_COMPILER_ID}) has not been tested on this project. Only Clang and GCC has been tested. Please report any problems at the project page on GitHub.")
endif()


include_directories(.)

find_package(Threads REQUIRED)

find_package(Boost 1.54.0 COMPONENTS regex system thread coroutine context filesystem date_time REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

add_executable(myscan myscan.cpp storage.cpp utils.cpp)
target_link_libraries(myscan ${Boost_LIBRARIES})
target_link_libraries(myscan ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(myscan mysqlcppconn)
target_link_libraries(myscan config++)

