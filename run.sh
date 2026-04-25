#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAME="keeply"
BUILD_TYPE="Release"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$DIR/build"
BIN_PATH="$BUILD_DIR/$APP_NAME"
PID_FILE="$BUILD_DIR/$APP_NAME.pid"

read_pid_file() {
  local pid=""
  [[ -f "$PID_FILE" ]] || return 1
  pid="$(tr -d '[:space:]' < "$PID_FILE" 2>/dev/null || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$pid"
}

has_foreground_flag() {
  local arg
  for arg in "$@"; do
    [[ "$arg" == "--foreground" ]] && return 0
  done
  return 1
}

validate_args() {
  local arg
  for arg in "$@"; do
    if [[ "$arg" != "--foreground" ]]; then
      fail "run.sh aceita apenas --foreground. O agente usa configuracao automatica e o nome da maquina."
    fi
  done
}

log() {
  echo "[Keeply Agente] $*"
}

fail() {
  echo "[Keeply Agente][ERRO] $*" >&2
  exit 1
}

cleanup_old_process() {
  local pids=""
  local old_pid=""

  if [[ -f "$PID_FILE" ]]; then
    old_pid="$(read_pid_file || true)"
    if [[ -n "${old_pid:-}" ]] && kill -0 "$old_pid" 2>/dev/null; then
      log "Processo anterior encontrado pelo PID file: $old_pid. Encerrando..."
      kill -TERM "$old_pid" 2>/dev/null || true

      for _ in {1..10}; do
        if ! kill -0 "$old_pid" 2>/dev/null; then
          break
        fi
        sleep 1
      done

      if kill -0 "$old_pid" 2>/dev/null; then
        log "Processo não encerrou com TERM. Forçando kill..."
        kill -KILL "$old_pid" 2>/dev/null || true
      fi
    fi
    rm -f "$PID_FILE"
  fi

  pids="$(pgrep -f "^$BIN_PATH( |$)" || true)"
  if [[ -n "$pids" ]]; then
    log "Instância(s) em execução encontrada(s): $pids. Encerrando..."
    pkill -TERM -f "^$BIN_PATH( |$)" || true

    for _ in {1..10}; do
      if ! pgrep -f "^$BIN_PATH( |$)" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done

    if pgrep -f "^$BIN_PATH( |$)" >/dev/null 2>&1; then
      log "Ainda existe processo ativo. Forçando encerramento..."
      pkill -KILL -f "^$BIN_PATH( |$)" || true
    fi
  else
    log "Nenhuma instância anterior encontrada."
  fi
}

check_dependencies() {
  command -v cmake >/dev/null 2>&1 || fail "cmake não encontrado."
  command -v make >/dev/null 2>&1 || fail "make não encontrado."
  command -v nproc >/dev/null 2>&1 || fail "nproc não encontrado."
}

build_app() {
  log "Preparando o ambiente de build..."
  mkdir -p "$BUILD_DIR"

  log "Configurando dependências via CMake..."
  cmake -S "$DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE"

  log "Compilando..."
  cmake --build "$BUILD_DIR" -j"$(nproc)"
}

run_app() {
  local launcher_pid=""
  local started_pid=""

  [[ -x "$BIN_PATH" ]] || fail "Binário não encontrado ou sem permissão de execução: $BIN_PATH"

  rm -f "$PID_FILE"

  if has_foreground_flag "$@"; then
    log "Iniciando o Agente em foreground..."
    exec env KEEPLY_AGENT_PID_FILE="$PID_FILE" "$BIN_PATH" --foreground
  fi

  log "Iniciando o Agente..."
  env KEEPLY_AGENT_PID_FILE="$PID_FILE" "$BIN_PATH" &
  launcher_pid=$!

  for _ in {1..50}; do
    started_pid="$(read_pid_file || true)"
    if [[ -n "$started_pid" ]] && kill -0 "$started_pid" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done

  if [[ -n "$started_pid" ]] && kill -0 "$started_pid" 2>/dev/null; then
    log "Agente iniciado com sucesso. PID: $started_pid"
    log "Verificação:"
    ps -p "$started_pid" -o pid,ppid,cmd --no-headers || true
  else
    wait "$launcher_pid" 2>/dev/null || true
    fail "O processo nao registrou o PID esperado e pode ter encerrado durante a inicializacao."
  fi
}

main() {
  cd "$DIR"
  validate_args "$@"
  check_dependencies
  cleanup_old_process
  build_app
  run_app "$@"
}

main "$@"
