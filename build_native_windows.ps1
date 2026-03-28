# =============================================================
# build_native_windows.ps1
# Instala MSYS2/MinGW-w64 e compila keeply_all.exe nativo Windows
# =============================================================
# Execute com:
#   powershell -ExecutionPolicy Bypass -File build_native_windows.ps1
# =============================================================

$ErrorActionPreference = "Stop"

$MSYS2_ROOT = "C:\msys64"
$MINGW_BIN  = "$MSYS2_ROOT\mingw64\bin"
$MSYS2_BASH = "$MSYS2_ROOT\usr\bin\bash.exe"

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$BUILD_DIR  = "$SCRIPT_DIR\build_win"

# ------------------------------------------------------------------
# 1. Instalar MSYS2 (se nao existir)
# ------------------------------------------------------------------
if (-not (Test-Path $MSYS2_ROOT)) {
    Write-Host "`n=== [1/4] Instalando MSYS2 via winget ===" -ForegroundColor Cyan
    winget install --id MSYS2.MSYS2 --source winget --accept-package-agreements --accept-source-agreements
    Start-Sleep -Seconds 8
    if (-not (Test-Path $MSYS2_ROOT)) {
        Write-Error "MSYS2 nao instalado em $MSYS2_ROOT. Instale manualmente em https://www.msys2.org"
    }
} else {
    Write-Host "`n=== [1/4] MSYS2 ja instalado em $MSYS2_ROOT ===" -ForegroundColor Green
}

# ------------------------------------------------------------------
# 2. Instalar pacotes MinGW64 via pacman (dentro do bash MSYS2)
# ------------------------------------------------------------------
Write-Host "`n=== [2/4] Instalando dependencias MinGW-w64 ===" -ForegroundColor Cyan

$pkgList = "mingw-w64-x86_64-gcc mingw-w64-x86_64-ninja mingw-w64-x86_64-openssl mingw-w64-x86_64-zlib mingw-w64-x86_64-sqlite3 mingw-w64-x86_64-zstd mingw-w64-x86_64-git base-devel"

& $MSYS2_BASH -lc "pacman -Syu --noconfirm --noprogressbar"
& $MSYS2_BASH -lc "pacman -S --needed --noconfirm --noprogressbar $pkgList"

# ------------------------------------------------------------------
# 3. Configurar CMake diretamente via PowerShell (evita problemas
#    de espacos no caminho que quebram o mingw32-make dentro do bash)
# ------------------------------------------------------------------
Write-Host "`n=== [3/4] Configurando CMake para MinGW (Ninja) ===" -ForegroundColor Cyan

# Adiciona MinGW ao PATH desta sessao PowerShell
$env:PATH = "$MINGW_BIN;$env:PATH"

# Remove build anterior se existir
if (Test-Path $BUILD_DIR) {
    Remove-Item -Recurse -Force $BUILD_DIR
}

$cmakeExe  = "$MINGW_BIN\cmake.exe"
$ninjaExe  = "$MINGW_BIN\ninja.exe"
$gccExe    = "$MINGW_BIN\gcc.exe"
$gppExe    = "$MINGW_BIN\g++.exe"

& $cmakeExe `
    -S "$SCRIPT_DIR" `
    -B "$BUILD_DIR" `
    -G "Ninja" `
    -DCMAKE_BUILD_TYPE=Release `
    -DCMAKE_C_COMPILER="$gccExe" `
    -DCMAKE_CXX_COMPILER="$gppExe" `
    -DCMAKE_MAKE_PROGRAM="$ninjaExe"

if ($LASTEXITCODE -ne 0) { Write-Error "Falha na configuracao do CMake." }

# ------------------------------------------------------------------
# 4. Compilar keeply_all.exe
# ------------------------------------------------------------------
Write-Host "`n=== [4/4] Compilando keeply_all.exe ===" -ForegroundColor Cyan

& $cmakeExe --build "$BUILD_DIR" --target keeply_all --parallel

if ($LASTEXITCODE -ne 0) { Write-Error "Falha na compilacao." }

# ------------------------------------------------------------------
# Resultado
# ------------------------------------------------------------------
Write-Host "`n=== Build concluido! ===" -ForegroundColor Green
Write-Host "Binario em: $BUILD_DIR\keeply_all.exe" -ForegroundColor Green

$exe = "$BUILD_DIR\keeply_all.exe"
if (Test-Path $exe) {
    $kb = [math]::Round((Get-Item $exe).Length / 1KB)
    Write-Host "  keeply_all.exe   $kb KB"
}

Write-Host "`nUso:" -ForegroundColor Yellow
Write-Host "  keeply_all.exe                   -> CLI (menu interativo)"
Write-Host "  keeply_all.exe backup            -> CLI backup"
Write-Host "  keeply_all.exe agent [--url .]   -> Agent WebSocket"
Write-Host "  keeply_all.exe cbt --root <dir>  -> CBT Daemon"
