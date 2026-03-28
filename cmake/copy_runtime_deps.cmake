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

# DLLs do sistema Windows que nunca precisam ser distribuidas
set(KEEPLY_SYSTEM_DLL_PATTERN
    "^(api-ms-win-|ext-ms-win-|KERNEL32|kernel32|KernelBase|kernelbase|ntdll|NTDLL|msvcrt|MSVCRT|ucrtbase|UCRTBASE|msvcp_win|vcruntime|user32|USER32|gdi32|GDI32|win32u|WIN32U|ws2_32|WS2_32|advapi32|ADVAPI32|shell32|SHELL32|ole32|OLE32|combase|COMBASE|rpcrt4|RPCRT4|sechost|SECHOST|winspool|comdlg32|oleaut32|uuid|shlwapi|SHLWAPI|imm32|IMM32|cfgmgr32|CFGMGR32|setupapi|SETUPAPI|crypt32|CRYPT32)")

foreach(dep IN LISTS keeply_resolved_deps)
    get_filename_component(dep_name "${dep}" NAME_WE)
    if(dep_name MATCHES "${KEEPLY_SYSTEM_DLL_PATTERN}")
        continue()
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${dep}" "${KEEPLY_RUNTIME_OUTPUT_DIR}"
        COMMAND_ERROR_IS_FATAL ANY
    )
    get_filename_component(dep_basename "${dep}" NAME)
    message(STATUS "DLL copiada: ${dep_basename}")
endforeach()

# Ignora silenciosamente as api-ms-win-* nas unresolved (sao DLLs virtuais do Windows)
if(keeply_unresolved_deps)
    set(keeply_real_unresolved)
    foreach(dep IN LISTS keeply_unresolved_deps)
        if(NOT dep MATCHES "^(api-ms-win-|ext-ms-win-)")
            list(APPEND keeply_real_unresolved "${dep}")
        endif()
    endforeach()
    if(keeply_real_unresolved)
        list(JOIN keeply_real_unresolved ", " keeply_unresolved_text)
        message(WARNING "Dependencias de runtime nao resolvidas: ${keeply_unresolved_text}")
    endif()
endif()
