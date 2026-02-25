cmake_minimum_required(VERSION 3.16)
project(keeply LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(SQLite3 REQUIRED)

find_library(ZSTD_LIB
    NAMES zstd libzstd.so.1 zstd.so.1
    PATHS /lib /usr/lib /lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu
)
if(NOT ZSTD_LIB)
    message(FATAL_ERROR "libzstd runtime nao encontrada (instale libzstd)")
endif()

include(FetchContent)
FetchContent_Declare(
    blake3_src
    GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
    GIT_TAG fb1411ebdbec5327cb838646e71cfd37beda8c12
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(blake3_src)

set(KEEPLY_CORE
    ApiRest/api.cpp
    Backup/compactar.cpp
    Backup/restore.cpp
    Backup/scan.cpp
    Backup/storage.cpp
    ${blake3_src_SOURCE_DIR}/c/blake3.c
    ${blake3_src_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_src_SOURCE_DIR}/c/blake3_portable.c
)

set(KEEPLY_API_WS
    ApiRest/api-rest.cpp
    WebSocket/ws.cpp
)

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
    main_cli.cpp
    ${KEEPLY_CORE}
)

set(KEEPLY_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/ApiRest
    ${CMAKE_CURRENT_SOURCE_DIR}/WebSocket
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

add_executable(keeply_api_ws_smoke
    HTTP/api_ws_smoke_main.cpp
    ${KEEPLY_CORE}
    ${KEEPLY_API_WS}
)

target_include_directories(keeply_api_ws_smoke PRIVATE ${KEEPLY_INCLUDE_DIRS})

target_compile_definitions(keeply_api_ws_smoke PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512
)

target_link_libraries(keeply_api_ws_smoke PRIVATE ${KEEPLY_COMMON_LIBS})

add_executable(keeply_rest_http
    HTTP/rest_http_main.cpp
    ${KEEPLY_CORE}
    ApiRest/api-rest.cpp
)

target_include_directories(keeply_rest_http PRIVATE ${KEEPLY_INCLUDE_DIRS})

target_compile_definitions(keeply_rest_http PRIVATE
    BLAKE3_USE_NEON=0
    BLAKE3_NO_AVX512
)

target_link_libraries(keeply_rest_http PRIVATE ${KEEPLY_COMMON_LIBS})
