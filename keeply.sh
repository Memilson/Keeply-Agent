#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

AGENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${AGENT_DIR}/build"
OUTPUT_FILE="${AGENT_DIR}/keeply-all.bin"
DEFAULT_WS_URL="ws://127.0.0.1:8081/ws/agent"

die(){ echo "Erro: $*" >&2; exit 1; }
log(){ echo "[keeply] $*"; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || die "comando nao encontrado: $1"; }

usage() {
cat <<EOF
Uso:
  ./Agente/keeply.sh build-agent
  ./Agente/keeply.sh build-installer [arquivo_saida]
EOF
}

build_agent() {
    need_cmd cmake
    need_cmd nproc
    mkdir -p "${BUILD_DIR}"
    log "configurando CMake em ${BUILD_DIR}"
    cmake -S "${AGENT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
    log "compilando keeply_agent e keeply_cbt_daemon"
    cmake --build "${BUILD_DIR}" --target keeply_agent keeply_cbt_daemon -j"${BUILD_JOBS:-$(nproc)}"
    [[ -x "${BUILD_DIR}/keeply_agent" ]] || die "binario nao gerado: ${BUILD_DIR}/keeply_agent"
    [[ -x "${BUILD_DIR}/keeply_cbt_daemon" ]] || die "binario nao gerado: ${BUILD_DIR}/keeply_cbt_daemon"
    log "binarios gerados:"
    echo "  ${BUILD_DIR}/keeply_agent"
    echo "  ${BUILD_DIR}/keeply_cbt_daemon"
}

build_installer() {
    need_cmd tar
    need_cmd mktemp
    need_cmd sed
    need_cmd wc
    need_cmd install
    local out="${1:-${OUTPUT_FILE}}"
    local tmp_dir stage_dir payload_tgz archive_line

    mkdir -p "$(dirname "${out}")"
    build_agent

    tmp_dir="$(mktemp -d)"
    cleanup(){ rm -rf "${tmp_dir}"; }
    trap cleanup RETURN

    stage_dir="${tmp_dir}/payload"
    mkdir -p "${stage_dir}/Agente/build"
    install -m 755 "${BUILD_DIR}/keeply_agent" "${stage_dir}/Agente/build/keeply_agent"
    install -m 755 "${BUILD_DIR}/keeply_cbt_daemon" "${stage_dir}/Agente/build/keeply_cbt_daemon"

    cat > "${stage_dir}/install_systemd_services.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

die(){ echo "Erro: $*" >&2; exit 1; }
log(){ echo "[keeply-install] $*"; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || die "comando nao encontrado: $1"; }
escape_env(){ printf '%q' "$1"; }
has_unit(){ systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -Fxq "$1"; }
stop_unit(){ has_unit "$1" && systemctl stop "$1" >/dev/null 2>&1 || true; }

[[ "${EUID}" -eq 0 ]] || die "Execute como root: sudo $0"

need_cmd systemctl
need_cmd getent
need_cmd groupadd
need_cmd useradd
need_cmd usermod
need_cmd install
need_cmd pkill
need_cmd journalctl

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATCH_ROOT="${1:-/}"
WS_URL="${2:-ws://127.0.0.1:8081/ws/agent}"
PREFIX="${3:-/opt/keeply}"
SYSTEMD_DIR="${4:-/etc/systemd/system}"
SERVICE_USER="${SERVICE_USER:-keeply}"
SERVICE_GROUP="${SERVICE_GROUP:-keeply}"
INSTALL_CBT_SERVICE="${INSTALL_CBT_SERVICE:-1}"
KEEPLY_HOME="${KEEPLY_HOME:-/var/lib/keeply}"
XDG_DATA_HOME_DIR="${XDG_DATA_HOME_DIR:-${KEEPLY_HOME}/.local/share}"
XDG_STATE_HOME_DIR="${XDG_STATE_HOME_DIR:-${KEEPLY_HOME}/.local/state}"
XDG_CACHE_HOME_DIR="${XDG_CACHE_HOME_DIR:-${KEEPLY_HOME}/.cache}"
KEEPLY_TMP_DIR="${KEEPLY_TMP_DIR:-${KEEPLY_HOME}/tmp}"
BIN_DIR="${PREFIX%/}/bin"
CBT_STATE_DIR="${XDG_STATE_HOME_DIR}/keeply"

AGENT_BIN_SRC="${PKG_DIR}/Agente/build/keeply_agent"
CBT_BIN_SRC="${PKG_DIR}/Agente/build/keeply_cbt_daemon"
AGENT_BIN_DST="${BIN_DIR}/keeply_agent"
CBT_BIN_DST="${BIN_DIR}/keeply_cbt_daemon"
AGENT_ENV_FILE="/etc/default/keeply-agent"
CBT_ENV_FILE="/etc/default/keeply-cbt-daemon"
AGENT_SERVICE_FILE="${SYSTEMD_DIR}/keeply-agent.service"
CBT_SERVICE_FILE="${SYSTEMD_DIR}/keeply-cbt-daemon.service"

[[ -x "${AGENT_BIN_SRC}" ]] || die "binario nao encontrado: ${AGENT_BIN_SRC}"
[[ -x "${CBT_BIN_SRC}" ]] || die "binario nao encontrado: ${CBT_BIN_SRC}"
[[ -d "${WATCH_ROOT}" || "${WATCH_ROOT}" == "/" ]] || die "WATCH_ROOT invalido: ${WATCH_ROOT}"
mkdir -p "${SYSTEMD_DIR}"

if ! getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
    log "criando grupo ${SERVICE_GROUP}"
    groupadd --system "${SERVICE_GROUP}"
fi

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    log "criando usuario ${SERVICE_USER}"
    useradd --system --create-home --home-dir "${KEEPLY_HOME}" --shell /usr/sbin/nologin --gid "${SERVICE_GROUP}" "${SERVICE_USER}"
else
    current_group="$(id -gn "${SERVICE_USER}")"
    [[ "${current_group}" == "${SERVICE_GROUP}" ]] || usermod -g "${SERVICE_GROUP}" "${SERVICE_USER}"
fi

stop_unit "keeply-agent.service"
stop_unit "keeply-cbt-daemon.service"
pkill -f "${AGENT_BIN_DST}" >/dev/null 2>&1 || true
pkill -f "${CBT_BIN_DST}" >/dev/null 2>&1 || true
pkill -f '/usr/local/bin/keeply_agent' >/dev/null 2>&1 || true
pkill -f '/usr/local/bin/keeply_cbt_daemon' >/dev/null 2>&1 || true

install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${BIN_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${KEEPLY_HOME}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_DATA_HOME_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_STATE_HOME_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_CACHE_HOME_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${KEEPLY_TMP_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${CBT_STATE_DIR}"

install -m 755 "${AGENT_BIN_SRC}" "${AGENT_BIN_DST}"
install -m 755 "${CBT_BIN_SRC}" "${CBT_BIN_DST}"

cat > "${AGENT_SERVICE_FILE}" <<EOF2
[Unit]
Description=Keeply Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
EnvironmentFile=-${AGENT_ENV_FILE}
ExecStart=${AGENT_BIN_DST} --foreground --no-tray
Restart=always
RestartSec=3
WorkingDirectory=${KEEPLY_HOME}

[Install]
WantedBy=multi-user.target
EOF2

if [[ "${INSTALL_CBT_SERVICE}" == "1" ]]; then
cat > "${CBT_SERVICE_FILE}" <<EOF2
[Unit]
Description=Keeply CBT Daemon
After=local-fs.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
EnvironmentFile=-${CBT_ENV_FILE}
ExecStart=${CBT_BIN_DST} --foreground
Restart=always
RestartSec=2
WorkingDirectory=${KEEPLY_HOME}

[Install]
WantedBy=multi-user.target
EOF2
else
    rm -f "${CBT_SERVICE_FILE}"
fi

mkdir -p /etc/default

cat > "${AGENT_ENV_FILE}" <<EOF2
KEEPLY_WS_URL=$(escape_env "${WS_URL}")
KEEPLY_ROOT=$(escape_env "${WATCH_ROOT}")
KEEPLY_DATA_DIR=$(escape_env "${KEEPLY_HOME}")
HOME=$(escape_env "${KEEPLY_HOME}")
XDG_DATA_HOME=$(escape_env "${XDG_DATA_HOME_DIR}")
XDG_STATE_HOME=$(escape_env "${XDG_STATE_HOME_DIR}")
XDG_CACHE_HOME=$(escape_env "${XDG_CACHE_HOME_DIR}")
TMPDIR=$(escape_env "${KEEPLY_TMP_DIR}")
EOF2

cat > "${CBT_ENV_FILE}" <<EOF2
KEEPLY_ROOT=$(escape_env "${WATCH_ROOT}")
HOME=$(escape_env "${KEEPLY_HOME}")
XDG_DATA_HOME=$(escape_env "${XDG_DATA_HOME_DIR}")
XDG_STATE_HOME=$(escape_env "${XDG_STATE_HOME_DIR}")
XDG_CACHE_HOME=$(escape_env "${XDG_CACHE_HOME_DIR}")
TMPDIR=$(escape_env "${KEEPLY_TMP_DIR}")
EOF2

chmod 644 "${AGENT_ENV_FILE}" "${CBT_ENV_FILE}"

rm -f "${CBT_STATE_DIR}/keeplyintf.pid" "${CBT_STATE_DIR}/keeplyintf.root"
systemctl daemon-reload

if [[ "${INSTALL_CBT_SERVICE}" == "1" ]]; then
    systemctl enable keeply-agent.service keeply-cbt-daemon.service >/dev/null
    systemctl restart keeply-cbt-daemon.service
    systemctl restart keeply-agent.service
else
    systemctl disable keeply-cbt-daemon.service >/dev/null 2>&1 || true
    systemctl enable keeply-agent.service >/dev/null
    systemctl restart keeply-agent.service
fi

failed=0
systemctl is-active --quiet keeply-agent.service || failed=1
if [[ "${INSTALL_CBT_SERVICE}" == "1" ]]; then
    systemctl is-active --quiet keeply-cbt-daemon.service || failed=1
fi

echo "Instalacao concluida."
echo "  root      : ${WATCH_ROOT}"
echo "  ws        : ${WS_URL}"
echo "  prefix    : ${PREFIX}"
echo "  systemd   : ${SYSTEMD_DIR}"
echo "  user/group: ${SERVICE_USER}:${SERVICE_GROUP}"
echo "  agent     : ${AGENT_BIN_DST}"
echo "  daemon    : ${CBT_BIN_DST}"
echo "  cbt_svc   : ${INSTALL_CBT_SERVICE}"

if [[ "${failed}" -ne 0 ]]; then
    echo >&2
    echo "Um ou mais servicos falharam ao iniciar." >&2
    systemctl status keeply-agent.service keeply-cbt-daemon.service --no-pager -l || true
    echo >&2
    journalctl -u keeply-agent.service -u keeply-cbt-daemon.service -n 80 --no-pager || true
    exit 1
fi
EOF
    chmod +x "${stage_dir}/install_systemd_services.sh"

    payload_tgz="${tmp_dir}/payload.tgz"
    tar -C "${stage_dir}" -czf "${payload_tgz}" .

    cat > "${tmp_dir}/installer.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ "${EUID}" -eq 0 ]] || { echo "Execute como root: sudo $0" >&2; exit 1; }

WATCH_ROOT="${1:-/}"
WS_URL="${2:-ws://127.0.0.1:8081/ws/agent}"
PREFIX="${3:-/opt/keeply}"
SYSTEMD_DIR="${4:-/etc/systemd/system}"

TMP_DIR="$(mktemp -d)"
cleanup(){ rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

ARCHIVE_LINE=$((__ARCHIVE_LINE__ + 1))
tail -n +"${ARCHIVE_LINE}" "$0" | tar -xz -C "${TMP_DIR}"
exec "${TMP_DIR}/install_systemd_services.sh" "${WATCH_ROOT}" "${WS_URL}" "${PREFIX}" "${SYSTEMD_DIR}"
__ARCHIVE_BELOW__
EOF

    archive_line="$(wc -l < "${tmp_dir}/installer.sh")"
    sed "s/__ARCHIVE_LINE__/${archive_line}/" "${tmp_dir}/installer.sh" > "${out}"
    cat "${payload_tgz}" >> "${out}"
    chmod +x "${out}"

    log "instalador gerado:"
    echo "  ${out}"
}

cmd="${1:-}"
[[ -n "${cmd}" ]] || { usage; exit 1; }
shift || true

case "${cmd}" in
    build-agent) build_agent "$@" ;;
    build-installer) build_installer "$@" ;;
    *) usage; exit 1 ;;
esac
