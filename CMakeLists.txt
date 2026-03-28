cmake_minimum_required(VERSION 3.16)
project(keeply LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MINGW)
    add_link_options(-static-libgcc -static-libstdc++)
endif()

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(SQLite3 REQUIRED)

find_library(ZSTD_LIB
    NAMES zstd libzstd zstd.so.1 libzstd.so.1 zstd_static libzstd_static
)
if(NOT ZSTD_LIB)
    message(FATAL_ERROR "Biblioteca zstd nao encontrada.")
endif()

include(FetchContent)
FetchContent_Declare(
    blake3_src
    GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
    GIT_TAG 1.5.1
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(blake3_src)

# =========================================================================
# Platform-specific sources
# =========================================================================

set(KEEPLY_CBT_PLATFORM_SOURCES)
if(UNIX AND NOT WIN32)
    list(APPEND KEEPLY_CBT_PLATFORM_SOURCES
        Backup/Rastreamento/Linux/daemon.cpp
    )
elseif(WIN32)
    list(APPEND KEEPLY_CBT_PLATFORM_SOURCES
        Backup/Rastreamento/Windows/daemon.cpp
    )
endif()

set(KEEPLY_PLATFORM_SOURCES
    ${KEEPLY_CBT_PLATFORM_SOURCES}
)

# =========================================================================
# Core — tipos, plataforma, compactação, db
# =========================================================================

set(KEEPLY_CORE
    Core/plataforma.cpp
    Core/compactacao.cpp
    ${KEEPLY_PLATFORM_SOURCES}
    API/keeply_api.cpp
    Backup/Rastreamento/rastreamento_mudancas.cpp
    Backup/verificacao.cpp
    Backup/restauracao.cpp
    Backup/varredura.cpp
    Storage/arquivo_armazenamento.cpp
    ${blake3_src_SOURCE_DIR}/c/blake3.c
    ${blake3_src_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_src_SOURCE_DIR}/c/blake3_portable.c
)

# =========================================================================
# Network — HTTP, WebSocket, Cloud upload
# =========================================================================

set(KEEPLY_API_WS
    HTTP/http_util.cpp
    Cloud/fila_upload.cpp
    WebSocket/websocket_agente.cpp
    HTTP/cliente_http.cpp
    WebSocket/inicializacao.cpp
)

if(UNIX OR WIN32)
    list(APPEND KEEPLY_API_WS
        HTTP/servidor_rest.cpp
    )
endif()

set(KEEPLY_AGENT_EXTRA_SOURCES)
if(UNIX OR WIN32)
    list(APPEND KEEPLY_AGENT_EXTRA_SOURCES
        ${KEEPLY_CBT_PLATFORM_SOURCES}
    )
endif()

# =========================================================================
# BLAKE3 SIMD
# =========================================================================

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64|i[3-6]86)$"
   AND CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    list(APPEND KEEPLY_CORE
        ${blake3_src_SOURCE_DIR}/c/blake3_sse2.c
        ${blake3_src_SOURCE_DIR}/c/blake3_sse41.c
        ${blake3_src_SOURCE_DIR}/c/blake3_avx2.c
    )
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_sse2.c
        PROPERTIES COMPILE_OPTIONS "-msse2"
    )
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_sse41.c
        PROPERTIES COMPILE_OPTIONS "-msse4.1"
    )
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_avx2.c
        PROPERTIES COMPILE_OPTIONS "-mavx2;-msse4.1"
    )
endif()

# =========================================================================
# Include directories
# =========================================================================

set(KEEPLY_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/Core
    ${CMAKE_CURRENT_SOURCE_DIR}/Storage
    ${CMAKE_CURRENT_SOURCE_DIR}/Backup
    ${CMAKE_CURRENT_SOURCE_DIR}/API
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTP
    ${CMAKE_CURRENT_SOURCE_DIR}/WebSocket
    ${CMAKE_CURRENT_SOURCE_DIR}/Cloud
    ${blake3_src_SOURCE_DIR}/c
)

set(KEEPLY_COMMON_LIBS
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
    SQLite::SQLite3
    ${ZSTD_LIB}
)

function(keeply_stage_windows_runtime_deps target_name)
    if(NOT WIN32)
        return()
    endif()

    add_custom_command(TARGET ${target_name} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
            -DKEEPLY_RUNTIME_TARGET=$<TARGET_FILE:${target_name}>
            -DKEEPLY_RUNTIME_OUTPUT_DIR=$<TARGET_FILE_DIR:${target_name}>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/copy_runtime_deps.cmake
        VERBATIM
    )
endfunction()

# =========================================================================
# Target: keeply (CLI)
# =========================================================================

add_executable(keeply
    keeply.cpp
    ${KEEPLY_CORE}
)

set_source_files_properties(keeply.cpp PROPERTIES COMPILE_DEFINITIONS KEEPLY_CLI_IMPLEMENTATION=1)

target_include_directories(keeply PRIVATE ${KEEPLY_INCLUDE_DIRS})

target_compile_definitions(keeply PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512
)

target_link_libraries(keeply PRIVATE ${KEEPLY_COMMON_LIBS})
keeply_stage_windows_runtime_deps(keeply)

# =========================================================================
# Target: keeply_agent (WebSocket agent)
# =========================================================================

add_executable(keeply_agent
    agente_main.cpp
    ${KEEPLY_CORE}
    ${KEEPLY_API_WS}
    ${KEEPLY_AGENT_EXTRA_SOURCES}
)

target_include_directories(keeply_agent PRIVATE ${KEEPLY_INCLUDE_DIRS})

target_compile_definitions(keeply_agent PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512
)

target_link_libraries(keeply_agent PRIVATE ${KEEPLY_COMMON_LIBS})
keeply_stage_windows_runtime_deps(keeply_agent)

# =========================================================================
# Platform link libraries
# =========================================================================

if(WIN32)
    target_link_libraries(keeply PRIVATE Shell32 Ole32 User32)
    target_link_libraries(keeply_agent PRIVATE ws2_32 Shell32 Ole32 User32)
endif()

# =========================================================================
# Target: keeply_cbt_daemon (Change tracking daemon)
# =========================================================================

if(UNIX AND NOT WIN32)
    add_executable(keeply_cbt_daemon
        Core/plataforma.cpp
        Backup/Rastreamento/rastreamento_mudancas.cpp
        ${KEEPLY_CBT_PLATFORM_SOURCES}
    )

    target_include_directories(keeply_cbt_daemon PRIVATE ${KEEPLY_INCLUDE_DIRS})

    target_compile_definitions(keeply_cbt_daemon PRIVATE
        KEEPLY_DAEMON_PROGRAM=1
        BLAKE3_USE_NEON=0
        BLAKE3_NO_AVX512
    )

    target_link_libraries(keeply_cbt_daemon PRIVATE
        SQLite::SQLite3
    )
elseif(WIN32)
    add_executable(keeply_cbt_daemon
        Core/plataforma.cpp
        Backup/Rastreamento/rastreamento_mudancas.cpp
        ${KEEPLY_CBT_PLATFORM_SOURCES}
    )

    target_include_directories(keeply_cbt_daemon PRIVATE ${KEEPLY_INCLUDE_DIRS})

    target_compile_definitions(keeply_cbt_daemon PRIVATE
        KEEPLY_DAEMON_PROGRAM=1
        BLAKE3_USE_NEON=0
        BLAKE3_NO_AVX512
    )

    target_link_libraries(keeply_cbt_daemon PRIVATE
        SQLite::SQLite3
        Advapi32
        Shell32
        Ole32
    )
endif()

if(WIN32)
    keeply_stage_windows_runtime_deps(keeply_cbt_daemon)
endif()
