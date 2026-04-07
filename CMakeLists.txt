cmake_minimum_required(VERSION 3.16)
project(keeply LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(SQLite3 REQUIRED)
find_library(ZSTD_LIB
    NAMES zstd libzstd zstd.so.1 libzstd.so.1 zstd_static libzstd_static)
if(NOT ZSTD_LIB)
    message(FATAL_ERROR "Biblioteca zstd nao encontrada.")
endif()
find_library(RSYNC_LIB NAMES rsync librsync)
find_path(RSYNC_INCLUDE_DIR NAMES librsync.h)
if(RSYNC_LIB AND RSYNC_INCLUDE_DIR)
    message(STATUS "librsync encontrada: ${RSYNC_LIB} com headers em ${RSYNC_INCLUDE_DIR} — deltas byte-level habilitados.")
else()
    message(STATUS "librsync NAO encontrada. Instale librsync-dev para suporte a deltas byte-level.")
endif()
include(FetchContent)
FetchContent_Declare(
    blake3_src
    GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
    GIT_TAG 1.5.1
    GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(blake3_src)
set(KEEPLY_CBT_PLATFORM_SOURCES)
list(APPEND KEEPLY_CBT_PLATFORM_SOURCES
    Backup/Rastreamento/Linux/daemon.cpp)
set(KEEPLY_CORE
    Core/plataforma.cpp
    Core/compactacao.cpp
    API/keeply_api.cpp
    Backup/Rastreamento/rastreamento_mudancas.cpp
    Backup/verificacao.cpp
    Backup/restauracao.cpp
    Backup/varredura.cpp
    Storage/arquivo_armazenamento.cpp
    ${blake3_src_SOURCE_DIR}/c/blake3.c
    ${blake3_src_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_src_SOURCE_DIR}/c/blake3_portable.c)
if(RSYNC_LIB AND RSYNC_INCLUDE_DIR)
    list(APPEND KEEPLY_CORE Backup/delta_rsync.cpp)
endif()
set(KEEPLY_API_WS
    HTTP/http_util.cpp
    Cloud/fila_upload.cpp
    WebSocket/websocket_agente.cpp
    HTTP/cliente_http.cpp
    WebSocket/inicializacao.cpp
    HTTP/servidor_rest.cpp)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64|i[3-6]86)$"
   AND CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    list(APPEND KEEPLY_CORE
        ${blake3_src_SOURCE_DIR}/c/blake3_sse2.c
        ${blake3_src_SOURCE_DIR}/c/blake3_sse41.c
        ${blake3_src_SOURCE_DIR}/c/blake3_avx2.c)
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_sse2.c
        PROPERTIES COMPILE_OPTIONS "-msse2")
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_sse41.c
        PROPERTIES COMPILE_OPTIONS "-msse4.1")
    set_source_files_properties(
        ${blake3_src_SOURCE_DIR}/c/blake3_avx2.c
        PROPERTIES COMPILE_OPTIONS "-mavx2;-msse4.1")
endif()
set(KEEPLY_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/Core
    ${CMAKE_CURRENT_SOURCE_DIR}/Storage
    ${CMAKE_CURRENT_SOURCE_DIR}/Backup
    ${CMAKE_CURRENT_SOURCE_DIR}/API
    ${CMAKE_CURRENT_SOURCE_DIR}/HTTP
    ${CMAKE_CURRENT_SOURCE_DIR}/WebSocket
    ${CMAKE_CURRENT_SOURCE_DIR}/Cloud
    ${blake3_src_SOURCE_DIR}/c)
set(KEEPLY_COMMON_LIBS
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
    SQLite::SQLite3
    ${ZSTD_LIB})
if(RSYNC_LIB AND RSYNC_INCLUDE_DIR)
    list(APPEND KEEPLY_COMMON_LIBS ${RSYNC_LIB})
    list(APPEND KEEPLY_INCLUDE_DIRS ${RSYNC_INCLUDE_DIR})
    add_compile_definitions(KEEPLY_HAVE_RSYNC=1)
else()
    add_compile_definitions(KEEPLY_HAVE_RSYNC=0)
endif()

add_executable(keeply
    keeply.cpp
    ${KEEPLY_CORE}
    ${KEEPLY_API_WS})
target_include_directories(keeply PRIVATE ${KEEPLY_INCLUDE_DIRS})
target_compile_definitions(keeply PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512)
target_link_libraries(keeply PRIVATE ${KEEPLY_COMMON_LIBS})
add_executable(keeply_cbt_daemon
    Core/plataforma.cpp
    Backup/Rastreamento/rastreamento_mudancas.cpp
    ${KEEPLY_CBT_PLATFORM_SOURCES})
target_include_directories(keeply_cbt_daemon PRIVATE ${KEEPLY_INCLUDE_DIRS})
target_compile_definitions(keeply_cbt_daemon PRIVATE
    KEEPLY_DAEMON_PROGRAM=1
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512)
target_link_libraries(keeply_cbt_daemon PRIVATE
    SQLite::SQLite3)
