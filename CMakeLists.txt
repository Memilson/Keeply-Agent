cmake_minimum_required(VERSION 3.16)
project(keeply LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
    GIT_TAG 6eebbbd67935cc6b8fe24da164ae968a2e3973cb
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(blake3_src)

set(KEEPLY_PLATFORM_SOURCES)
set(KEEPLY_CBT_COMMON_SOURCES)

set(KEEPLY_CBT_PLATFORM_SOURCES)
if(UNIX AND NOT WIN32)
    list(APPEND KEEPLY_CBT_PLATFORM_SOURCES
        Backup/Linux/daemon_rastreamento_mudancas.cpp
    )
elseif(WIN32)
    list(APPEND KEEPLY_CBT_PLATFORM_SOURCES
        Backup/Windows/daemon_rastreamento_mudancas.cpp
    )
endif()

set(KEEPLY_PLATFORM_SOURCES
    ${KEEPLY_CBT_PLATFORM_SOURCES}
)

set(KEEPLY_CORE
    keeply_impl.cpp
    ${KEEPLY_PLATFORM_SOURCES}
    HTTP/API/api_agente.cpp
    Backup/rastreamento_mudancas.cpp
    Backup/compactacao_e_hash.cpp
    Backup/verificacao_integridade.cpp
    Backup/restauracao.cpp
    Backup/varredura_backup.cpp
    Backup/Armazenamentos/arquivo_armazenamento.cpp
    ${blake3_src_SOURCE_DIR}/c/blake3.c
    ${blake3_src_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_src_SOURCE_DIR}/c/blake3_portable.c
)

set(KEEPLY_API_WS
    HTTP/http_interno.cpp
    HTTP/fila_upload_paralela.cpp
    HTTP/WebSocket/websocket_agente.cpp
    HTTP/WebSocket/cliente_http_agente.cpp
    HTTP/WebSocket/inicializacao_websocket_agente.cpp
)

if(UNIX OR WIN32)
    list(APPEND KEEPLY_API_WS
        HTTP/API/servidor_rest.cpp
    )
endif()

set(KEEPLY_AGENT_EXTRA_SOURCES)
if(UNIX OR WIN32)
    list(APPEND KEEPLY_AGENT_EXTRA_SOURCES
        ${KEEPLY_CBT_COMMON_SOURCES}
        ${KEEPLY_CBT_PLATFORM_SOURCES}
    )
endif()

set(BLAKE3_SIMD_SOURCES)
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

add_executable(keeply
    keeply.cpp
    ${KEEPLY_CORE}
)

set_source_files_properties(keeply.cpp PROPERTIES COMPILE_DEFINITIONS KEEPLY_CLI_IMPLEMENTATION=1)

set(KEEPLY_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTP
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTP/API
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTP/WebSocket
    ${blake3_src_SOURCE_DIR}/c
)

set(KEEPLY_COMMON_LIBS
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
    SQLite::SQLite3
    ${ZSTD_LIB}
)

target_include_directories(keeply PRIVATE ${KEEPLY_INCLUDE_DIRS})

target_compile_definitions(keeply PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512
)

target_link_libraries(keeply PRIVATE ${KEEPLY_COMMON_LIBS})

add_executable(keeply_agent
    HTTP/WebSocket/agente_websocket_main.cpp
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

if(WIN32)
    target_link_libraries(keeply PRIVATE Shell32 Ole32)
    target_link_libraries(keeply_agent PRIVATE ws2_32 Shell32 Ole32)
endif()

if(UNIX AND NOT WIN32)
    add_executable(keeply_cbt_daemon
        keeply_impl.cpp
        Backup/rastreamento_mudancas.cpp
        ${KEEPLY_CBT_COMMON_SOURCES}
        ${KEEPLY_CBT_PLATFORM_SOURCES}
        ${KEEPLY_PLATFORM_SOURCES}
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
        keeply_impl.cpp
        Backup/rastreamento_mudancas.cpp
        ${KEEPLY_CBT_COMMON_SOURCES}
        ${KEEPLY_CBT_PLATFORM_SOURCES}
        ${KEEPLY_PLATFORM_SOURCES}
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
