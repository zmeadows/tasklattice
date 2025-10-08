cmake_minimum_required(VERSION 3.20)

project(
  cpp_mandel
  VERSION 0.1.0
  DESCRIPTION "Tiny C++20 Mandelbrot CLI producing a CSV"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- File lists ---------------------------------------------------------------
set(CPP_MANDEL_HEADERS include/mandel/core.hpp)

set(CPP_MANDEL_CORE_SOURCES src/core.cpp)

set(CPP_MANDEL_CLI_SOURCES src/cli_main.cpp)

# Combine for convenience
set(CPP_MANDEL_ALL_FILES ${CPP_MANDEL_HEADERS} ${CPP_MANDEL_CORE_SOURCES}
                         ${CPP_MANDEL_CLI_SOURCES})

# --- Library ------------------------------------------------------------------
add_library(mandel STATIC ${CPP_MANDEL_CORE_SOURCES} ${CPP_MANDEL_HEADERS})

target_include_directories(
  mandel PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                $<INSTALL_INTERFACE:include>)

target_compile_features(mandel PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(mandel PRIVATE /W4 /permissive- /Zc:preprocessor)
else()
  target_compile_options(mandel PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
endif()

# --- Exe ----------------------------------------------------------------------
add_executable(mandel_cli ${CPP_MANDEL_CLI_SOURCES})

target_link_libraries(mandel_cli PRIVATE mandel)

target_compile_features(mandel_cli PRIVATE cxx_std_20)

# --- Misc ---------------------------------------------------------------------
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${CPP_MANDEL_ALL_FILES})
