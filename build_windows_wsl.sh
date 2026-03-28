#!/usr/bin/env bash
# =============================================================
# build_windows_wsl.sh
# Instala dependencias e compila keeply, keeply_agent e
# keeply_cbt_daemon via WSL2 (Ubuntu 24.04)
# =============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

echo "=== [1/3] Instalando dependencias de compilacao ==="
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    libssl-dev \
    zlib1g-dev \
    libsqlite3-dev \
    libzstd-dev \
    git

echo ""
echo "=== [2/3] Configurando CMake ==="
rm -rf "$BUILD_DIR"
cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release

echo ""
echo "=== [3/3] Compilando todos os targets ==="
cmake --build "$BUILD_DIR" --parallel "$(nproc)"

echo ""
echo "=== Build concluido! Binarios em: $BUILD_DIR ==="
ls -lh "$BUILD_DIR/keeply" "$BUILD_DIR/keeply_agent" "$BUILD_DIR/keeply_cbt_daemon" 2>/dev/null || true
