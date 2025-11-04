cmake_minimum_required(VERSION 3.16)
project(ChronoFeed VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(CHRONOFEED_BUILD_TESTS "Build the Google Test suite" ON)
option(CHRONOFEED_BUILD_TOOLS "Build the load-generating publisher" ON)

find_package(Threads REQUIRED)

add_compile_options(-Wall -Wextra -Wpedantic)

add_library(chronofeed_core
  src/config.cpp
  src/log.cpp
  src/payload.cpp
  src/mqtt/packet.cpp
  src/mqtt/client.cpp
)

target_include_directories(chronofeed_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(chronofeed_core PUBLIC Threads::Threads)
