if(NOT DEFINED KEEPLY_RUNTIME_TARGET OR KEEPLY_RUNTIME_TARGET STREQUAL "")
    message(FATAL_ERROR "KEEPLY_RUNTIME_TARGET nao informado.")
endif()

if(NOT DEFINED KEEPLY_RUNTIME_OUTPUT_DIR OR KEEPLY_RUNTIME_OUTPUT_DIR STREQUAL "")
    message(FATAL_ERROR "KEEPLY_RUNTIME_OUTPUT_DIR nao informado.")
endif()

file(GET_RUNTIME_DEPENDENCIES
    EXECUTABLES "${KEEPLY_RUNTIME_TARGET}"
    RESOLVED_DEPENDENCIES_VAR keeply_resolved_deps
    UNRESOLVED_DEPENDENCIES_VAR keeply_unresolved_deps
)

foreach(dep IN LISTS keeply_resolved_deps)
    get_filename_component(dep_name "${dep}" NAME)
    if(dep_name MATCHES "^(api-ms-win-|ext-ms-win-|KERNEL32\\.dll|USER32\\.dll|ADVAPI32\\.dll|SHELL32\\.dll|OLE32\\.dll|WS2_32\\.dll)$")
        continue()
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${dep}" "${KEEPLY_RUNTIME_OUTPUT_DIR}"
        COMMAND_ERROR_IS_FATAL ANY
    )
endforeach()

if(keeply_unresolved_deps)
    list(JOIN keeply_unresolved_deps ", " keeply_unresolved_deps_text)
    message(WARNING "Dependencias de runtime nao resolvidas para ${KEEPLY_RUNTIME_TARGET}: ${keeply_unresolved_deps_text}")
endif()
