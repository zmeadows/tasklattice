cmake_minimum_required(VERSION 3.20...3.30)

project(
  cpp_mandel
  VERSION 0.1.0
  DESCRIPTION "Tiny C++20 Mandelbrot CLI producing a CSV"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
endif()

if(MSVC)
  # Enable embedded debug info for MSVC so sccache can cache objects
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT Embedded)
endif()

# --- File lists ---------------------------------------------------------------
set(CPP_MANDEL_HEADERS include/mandel/core.hpp)
set(CPP_MANDEL_CORE_SOURCES src/core.cpp)
set(CPP_MANDEL_CLI_SOURCES src/cli_main.cpp)
set(CPP_MANDEL_ALL_FILES ${CPP_MANDEL_HEADERS} ${CPP_MANDEL_CORE_SOURCES}
                         ${CPP_MANDEL_CLI_SOURCES})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${CPP_MANDEL_ALL_FILES})

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

# --- External Dependencies ----------------------------------------------------
include(FetchContent)

# target: nlohmann_json::nlohmann_json
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.12.0)
FetchContent_MakeAvailable(nlohmann_json)

# target: tomlplusplus::tomlplusplus
FetchContent_Declare(
  tomlplusplus
  GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
  GIT_TAG v3.4.0)
FetchContent_MakeAvailable(tomlplusplus)

# target: yaml-cpp
set(YAML_BUILD_SHARED_LIBS
    OFF
    CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS
    OFF
    CACHE BOOL "" FORCE)

FetchContent_Declare(
  yaml_cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG master)
FetchContent_MakeAvailable(yaml_cpp)

# target: pugixml::pugixml
set(PUGIXML_BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(PUGIXML_BUILD_SAMPLES
    OFF
    CACHE BOOL "" FORCE)

FetchContent_Declare(
  pugixml
  GIT_REPOSITORY https://github.com/zeux/pugixml.git
  GIT_TAG v1.15)
FetchContent_MakeAvailable(pugixml)

# --- Exe ----------------------------------------------------------------------
add_executable(mandel_cli ${CPP_MANDEL_CLI_SOURCES})

target_compile_features(mandel_cli PRIVATE cxx_std_20)

target_link_libraries(mandel_cli PRIVATE mandel nlohmann_json::nlohmann_json)
target_link_libraries(mandel_cli PRIVATE tomlplusplus::tomlplusplus)
target_link_libraries(mandel_cli PRIVATE yaml-cpp)
target_link_libraries(mandel_cli PRIVATE pugixml::pugixml)

# --- CTest --------------------------------------------------------------------
include(CTest)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
