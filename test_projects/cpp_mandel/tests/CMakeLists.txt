enable_testing()

# Path to the driver script (the commented one we wrote earlier)
set(SMOKE_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/smoke.cmake")

# 1) Always run a CLI-only smoke with tiny dimensions
add_test(
  NAME smoke_cli_flags
  COMMAND
    ${CMAKE_COMMAND} -DCLI=$<TARGET_FILE:mandel_cli>
    -DOUT=${CMAKE_BINARY_DIR}/smoke_flags.csv -DWIDTH=8 -DHEIGHT=6 -DMAXIT=10
    -P ${SMOKE_SCRIPT})

# 2) Optional per-config smokes if files exist in source/configs/
set(CONFIG_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../configs")
foreach(cfg_name IN ITEMS config.json config.toml config.yaml config.yml
                          config.xml)
  set(cfg_path "${CONFIG_DIR}/${cfg_name}")
  if(EXISTS "${cfg_path}")
    string(REPLACE "." "_" cfg_id "${cfg_name}") # e.g., config_toml
    add_test(
      NAME smoke_${cfg_id}
      COMMAND
        ${CMAKE_COMMAND} -DCLI=$<TARGET_FILE:mandel_cli>
        -DOUT=${CMAKE_BINARY_DIR}/smoke_${cfg_id}.csv -DCONFIG=${cfg_path}
        -DWIDTH=8 -DHEIGHT=6 -DMAXIT=10 -P ${SMOKE_SCRIPT})
  endif()
endforeach()
