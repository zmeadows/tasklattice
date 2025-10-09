# cmake-format: off
# ------------------------------------------------------------------------------
# tests/smoke.cmake
#
# A tiny, cross-platform CMake script used as a CTest "driver" for a smoke test.
# We run the mandel_cli executable with a very small image, then validate that:
#   1) the CSV file exists
#   2) the header is exactly: px,py,x,y
#   3) the number of data rows equals width * height
#
# Variables (passed by add_test(... COMMAND cmake -D... -P smoke.cmake)):
#   CLI    : full path to the mandel_cli executable              (REQUIRED)
#   OUT    : full path to the output CSV to write                (REQUIRED)
#   WIDTH  : width in pixels; tiny for speed (default: 8)        (OPTIONAL)
#   HEIGHT : height in pixels; tiny for speed (default: 6)       (OPTIONAL)
#   MAXIT  : max iterations; also tiny (default: 10)             (OPTIONAL)
#   CONFIG : optional path to a config file (.json/.toml/.yaml/.yml/.xml)
#
# Any failure calls message(FATAL_ERROR ...) so the CTest test fails.
# ------------------------------------------------------------------------------
# cmake-format: on

# ---- sanity: check required inputs -------------------------------------------
if(NOT DEFINED CLI OR CLI STREQUAL "")
  message(
    FATAL_ERROR
      "smoke.cmake: CLI path not provided. Pass -DCLI=<path-to-mandel_cli>")
endif()

if(NOT DEFINED OUT OR OUT STREQUAL "")
  message(
    FATAL_ERROR
      "smoke.cmake: OUT path not provided. Pass -DOUT=<csv-output-path>")
endif()

# ---- defaults for optional knobs ---------------------------------------------
if(NOT DEFINED WIDTH)
  set(WIDTH 8)
endif()
if(NOT DEFINED HEIGHT)
  set(HEIGHT 6)
endif()
if(NOT DEFINED MAXIT)
  set(MAXIT 10)
endif()

# ---- build the command line --------------------------------------------------
# Always pass width/height/max-iters/out to keep the run tiny and fast. If
# CONFIG is provided, we put it first; CLI flags still override config values.
set(launch_cmd "${CLI}")
if(DEFINED CONFIG AND NOT CONFIG STREQUAL "")
  list(APPEND launch_cmd --config "${CONFIG}")
endif()
list(
  APPEND
  launch_cmd
  --width
  "${WIDTH}"
  --height
  "${HEIGHT}"
  --max-iters
  "${MAXIT}"
  --out
  "${OUT}")

# ---- run the CLI -------------------------------------------------------------
# We capture stdout/stderr and return code for helpful failure messages.
execute_process(
  COMMAND ${launch_cmd}
  RESULT_VARIABLE run_rv
  OUTPUT_VARIABLE run_out
  ERROR_VARIABLE run_err)

if(NOT run_rv EQUAL 0)
  message(
    FATAL_ERROR
      "mandel_cli exited with non-zero status (${run_rv})\n"
      "Command : ${launch_cmd}\n" "STDOUT  :\n${run_out}\n"
      "STDERR  :\n${run_err}\n")
endif()

# ---- validate CSV exists -----------------------------------------------------
if(NOT EXISTS "${OUT}")
  message(FATAL_ERROR "CSV not produced at expected path: ${OUT}")
endif()

# ---- read file lines (portable) ----------------------------------------------
# file(STRINGS) reads the file into a CMake list, one entry per line. This is
# fine for tiny smokes; for huge outputs you'd stream differently.
file(STRINGS "${OUT}" LINES)
list(LENGTH LINES LINE_COUNT)
if(LINE_COUNT LESS 1)
  message(FATAL_ERROR "CSV appears empty or missing header: ${OUT}")
endif()

# ---- header must match exactly -----------------------------------------------
list(GET LINES 0 header)
if(NOT header STREQUAL "px,py,x,y")
  message(FATAL_ERROR "CSV header mismatch.\n" "Expected: px,py,x,y\n"
                      "Actual  : ${header}\n" "File    : ${OUT}")
endif()

# ---- number of data rows must equal width*height -----------------------------
math(EXPR pixels "${WIDTH} * ${HEIGHT}")
math(EXPR data_rows "${LINE_COUNT} - 1")

if(NOT data_rows EQUAL pixels)
  message(
    FATAL_ERROR
      "CSV row count mismatch.\n"
      "Expected data rows: ${pixels} (width*height = ${WIDTH}*${HEIGHT})\n"
      "Actual data rows  : ${data_rows}\n"
      "Total lines       : ${LINE_COUNT} (includes 1 header)\n"
      "File              : ${OUT}")
endif()

# ---- success -----------------------------------------------------------------
message(STATUS "Smoke OK: ${OUT}  (${WIDTH}x${HEIGHT} -> ${data_rows} rows)")
