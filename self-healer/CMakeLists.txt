cmake_minimum_required(VERSION 3.20)

project(job_queue_self_healer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(self-healer
    src/main.cpp
    src/detectors.cpp
    src/healing_strategies.cpp
    src/json_writer.cpp
)

target_include_directories(self-healer PRIVATE src)

if(MSVC)
    target_compile_options(self-healer PRIVATE /W4)
else()
    target_compile_options(self-healer PRIVATE -Wall -Wextra -Wpedantic)
endif()
