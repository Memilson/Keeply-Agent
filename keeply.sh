#!/usr/bin/env bash
set -Eeuo pipefail
umask 022
AGENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${AGENT_DIR}/build"
OUTPUT_FILE="${AGENT_DIR}/keeply-all.bin"
DEFAULT_WS_URL="wss://backend.keeply.app.br/ws/agent"
die(){ echo "Erro: $*" >&2; exit 1; }
log(){ echo "[keeply] $*"; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || die "comando nao encontrado: $1"; }
usage() {
cat <<EOF
Uso:
  ./Agente/keeply.sh build-agent
  ./Agente/keeply.sh build-installer [arquivo_saida]
  ./Agente/keeply.sh verify [prefixo]
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
    need_cmd sha256sum
    local out="${1:-${OUTPUT_FILE}}"
    local tmp_dir stage_dir payload_tgz archive_line payload_hash installer_hash
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
escape_systemd_env(){
    local v="$1"
    if [[ "${v}" =~ [[:space:]\"\'\\\$\`\!] ]]; then
        v="${v//\\/\\\\}"
        v="${v//\"/\\\"}"
        v="${v//\$/\\\$}"
        v="${v//\`/\\\`}"
        printf '"%s"' "${v}"
    else
        printf '%s' "${v}"
    fi
}
has_unit(){ systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -Fxq "$1"; }
stop_unit(){ has_unit "$1" && systemctl stop "$1" >/dev/null 2>&1 || true; }
[[ "${EUID}" -eq 0 ]] || die "Execute como root: sudo $0"
need_cmd systemctl
need_cmd getent
need_cmd groupadd
need_cmd useradd
need_cmd usermod
need_cmd install
need_cmd ps
need_cmd journalctl
need_cmd sed
need_cmd id
need_cmd mktemp
read_identity_value(){
    local meta_file="$1" key="$2"
    [[ -f "${meta_file}" ]] || return 1
    sed -n "s/^${key}=//p" "${meta_file}" | tail -n 1
}
read_pairing_code(){ read_identity_value "$1" "pairing_code"; }
read_device_id(){ read_identity_value "$1" "device_id"; }
PAIRING_POPUP_PID=""
PAIRING_POPUP_SCRIPT=""
PAIRING_POPUP_LOG=""
close_pairing_popup(){
    if [[ -n "${PAIRING_POPUP_PID}" ]]; then
        kill -- "-${PAIRING_POPUP_PID}" >/dev/null 2>&1 || kill "${PAIRING_POPUP_PID}" >/dev/null 2>&1 || true
        wait "${PAIRING_POPUP_PID}" >/dev/null 2>&1 || true
        PAIRING_POPUP_PID=""
    fi
    [[ -z "${PAIRING_POPUP_SCRIPT}" ]] || { rm -f "${PAIRING_POPUP_SCRIPT}" 2>/dev/null || true; PAIRING_POPUP_SCRIPT=""; }
    [[ -z "${PAIRING_POPUP_LOG}" ]] || { rm -f "${PAIRING_POPUP_LOG}" 2>/dev/null || true; PAIRING_POPUP_LOG=""; }
}
trap close_pairing_popup EXIT INT TERM
log_popup_fallback(){
    local reason="$1" detail="${2:-}"
    log "popup fallback: ${reason}"
    [[ -z "${detail}" ]] || log "popup detalhe: ${detail}"
}
read_process_env_value(){
    local pid="$1" key="$2" env_file value
    [[ -n "${pid}" ]] || { printf '%s\n' ""; return 0; }
    env_file="/proc/${pid}/environ"
    [[ -r "${env_file}" ]] || { printf '%s\n' ""; return 0; }
    value="$(tr '\0' '\n' < "${env_file}" | sed -n "s/^${key}=//p" | head -n 1)"
    printf '%s\n' "${value}"
}
find_graphical_user(){
    if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
        printf '%s\n' "${SUDO_USER}"
        return 0
    fi
    if command -v logname >/dev/null 2>&1; then
        local login_name
        login_name="$(logname 2>/dev/null || true)"
        if [[ -n "${login_name}" && "${login_name}" != "root" ]]; then
            printf '%s\n' "${login_name}"
            return 0
        fi
    fi
    if command -v loginctl >/dev/null 2>&1; then
        local session uid user seat tty state remote service
        while read -r session uid user seat tty state remote service; do
            [[ -n "${session}" && "${state}" == "active" ]] || continue
            [[ -n "${user}" && "${user}" != "root" ]] || continue
            printf '%s\n' "${user}"
            return 0
        done < <(loginctl list-sessions --no-legend 2>/dev/null || true)
    fi
    printf '%s\n' ""
}
find_graphical_session_id(){
    local popup_user="$1"
    command -v loginctl >/dev/null 2>&1 || { printf '%s\n' ""; return 0; }
    local session uid user seat tty state remote service
    while read -r session uid user seat tty state remote service; do
        [[ -n "${session}" && "${state}" == "active" ]] || continue
        [[ -z "${popup_user}" || "${user}" == "${popup_user}" ]] || continue
        printf '%s\n' "${session}"
        return 0
    done < <(loginctl list-sessions --no-legend 2>/dev/null || true)
    printf '%s\n' ""
}
find_session_leader_pid(){
    local session_id="$1"
    [[ -n "${session_id}" ]] || { printf '%s\n' ""; return 0; }
    command -v loginctl >/dev/null 2>&1 || { printf '%s\n' ""; return 0; }
    loginctl show-session "${session_id}" -p Leader --value 2>/dev/null || true
}
read_session_env_value(){
    local session_id="$1" key="$2" leader_pid
    leader_pid="$(find_session_leader_pid "${session_id}")"
    read_process_env_value "${leader_pid}" "${key}"
}
find_xauthority_path(){
    local popup_user="$1" popup_uid="$2" popup_session="$3" candidate
    if [[ -n "${XAUTHORITY:-}" && -f "${XAUTHORITY}" ]]; then
        printf '%s\n' "${XAUTHORITY}"
        return 0
    fi
    candidate="$(read_session_env_value "${popup_session}" "XAUTHORITY")"
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    if [[ -n "${popup_user}" ]]; then
        candidate="$(read_user_process_env_value "${popup_user}" "XAUTHORITY")"
        if [[ -n "${candidate}" && -f "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    fi
    for candidate in \
        "/run/user/${popup_uid}/gdm/Xauthority" \
        "/run/user/${popup_uid}/.Xauthority" \
        "/home/${popup_user}/.Xauthority"; do
        [[ -f "${candidate}" ]] || continue
        printf '%s\n' "${candidate}"
        return 0
    done
    candidate="$(compgen -G "/run/user/${popup_uid}/.mutter-Xwaylandauth.*" 2>/dev/null | head -n 1 || true)"
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    printf '%s\n' ""
}
read_user_process_env_value(){
    local popup_user="$1" key="$2" pid env_file value
    while IFS= read -r pid; do
        value="$(read_process_env_value "${pid}" "${key}")"
        if [[ -n "${value}" ]]; then
            printf '%s\n' "${value}"
            return 0
        fi
    done < <(ps -u "${popup_user}" -o pid= 2>/dev/null | awk '{print $1}')
    printf '%s\n' ""
}
find_display_value(){
    local popup_user="$1" popup_uid="$2" popup_session="$3" candidate
    if [[ -n "${DISPLAY:-}" ]]; then
        printf '%s\n' "${DISPLAY}"
        return 0
    fi
    candidate="$(read_session_env_value "${popup_session}" "DISPLAY")"
    if [[ -n "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    if [[ -n "${popup_user}" ]]; then
        candidate="$(read_user_process_env_value "${popup_user}" "DISPLAY")"
        if [[ -n "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    fi
    for candidate in /tmp/.X11-unix/X*; do
        [[ -S "${candidate}" ]] || continue
        printf ':%s\n' "${candidate##*/X}"
        return 0
    done
    printf '%s\n' ""
}
find_wayland_display(){
    local popup_user="$1" popup_session="$2" candidate
    if [[ -n "${WAYLAND_DISPLAY:-}" ]]; then
        printf '%s\n' "${WAYLAND_DISPLAY}"
        return 0
    fi
    candidate="$(read_session_env_value "${popup_session}" "WAYLAND_DISPLAY")"
    if [[ -n "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    if [[ -n "${popup_user}" ]]; then
        candidate="$(read_user_process_env_value "${popup_user}" "WAYLAND_DISPLAY")"
        if [[ -n "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    fi
    printf '%s\n' ""
}
find_xdg_runtime_dir(){
    local popup_user="$1" popup_uid="$2" popup_session="$3" candidate
    if [[ -n "${XDG_RUNTIME_DIR:-}" && -d "${XDG_RUNTIME_DIR}" ]]; then
        printf '%s\n' "${XDG_RUNTIME_DIR}"
        return 0
    fi
    candidate="$(read_session_env_value "${popup_session}" "XDG_RUNTIME_DIR")"
    if [[ -n "${candidate}" && -d "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    if [[ -n "${popup_user}" ]]; then
        candidate="$(read_user_process_env_value "${popup_user}" "XDG_RUNTIME_DIR")"
        if [[ -n "${candidate}" && -d "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    fi
    if [[ -n "${popup_uid}" && -d "/run/user/${popup_uid}" ]]; then
        printf '/run/user/%s\n' "${popup_uid}"
        return 0
    fi
    printf '%s\n' ""
}
find_dbus_address(){
    local popup_user="$1" popup_uid="$2" popup_session="$3" popup_runtime_dir="$4" candidate
    if [[ -n "${DBUS_SESSION_BUS_ADDRESS:-}" ]]; then
        printf '%s\n' "${DBUS_SESSION_BUS_ADDRESS}"
        return 0
    fi
    candidate="$(read_session_env_value "${popup_session}" "DBUS_SESSION_BUS_ADDRESS")"
    if [[ -n "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
    fi
    if [[ -n "${popup_user}" ]]; then
        candidate="$(read_user_process_env_value "${popup_user}" "DBUS_SESSION_BUS_ADDRESS")"
        if [[ -n "${candidate}" ]]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    fi
    if [[ -n "${popup_runtime_dir}" && -S "${popup_runtime_dir}/bus" ]]; then
        printf 'unix:path=%s/bus\n' "${popup_runtime_dir}"
        return 0
    fi
    if [[ -n "${popup_uid}" && -S "/run/user/${popup_uid}/bus" ]]; then
        printf 'unix:path=/run/user/%s/bus\n' "${popup_uid}"
        return 0
    fi
    printf '%s\n' ""
}
write_pairing_popup_script(){
    local script_path
    script_path="$(mktemp /tmp/keeply-popup-XXXXXX.py)"
    cat > "${script_path}" <<'PY'
import sys, tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk
code = sys.argv[1] if len(sys.argv) > 1 else ""
device = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "Keeply Agent"
host = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else "desconhecido"
root = tk.Tk()
root.title("Keeply - Ativacao")
root.configure(bg="#ffffff")
root.resizable(False, False)
root.attributes("-topmost", True)
families = set(tkfont.families())
font_family = "Segoe UI" if "Segoe UI" in families else "DejaVu Sans"
code_font_family = "DejaVu Sans Mono" if "DejaVu Sans Mono" in families else font_family
style = ttk.Style(root)
try:
    if "clam" in style.theme_names():
        style.theme_use("clam")
except Exception:
    pass
style.configure("Keeply.Horizontal.TProgressbar", troughcolor="#dbeafe", background="#1e88e5", lightcolor="#1e88e5", darkcolor="#1565c0", bordercolor="#bfdbfe")
frame = tk.Frame(root, bg="#ffffff", padx=34, pady=28)
frame.pack(fill="both", expand=True)
tk.Label(frame, text="Keeply - Ativacao", bg="#ffffff", fg="#1565c0", font=(font_family, 18, "bold")).pack(pady=(0, 22))
tk.Label(frame, text="Codigo de ativacao Keeply", bg="#ffffff", fg="#475569", font=(font_family, 12)).pack(anchor="w")
code_box = tk.Frame(frame, bg="#eff6ff", highlightbackground="#bfdbfe", highlightcolor="#bfdbfe", highlightthickness=2, bd=0, padx=18, pady=14)
code_box.pack(fill="x", pady=(10, 18))
tk.Label(code_box, text=code, bg="#eff6ff", fg="#1565c0", font=(code_font_family, 19, "bold")).pack(anchor="center")
info = tk.Label(frame, text=f"Dispositivo: {device}\nHost: {host}", justify="left", bg="#ffffff", fg="#334155", font=(font_family, 12))
info.pack(anchor="w", pady=(0, 18))
hint = tk.Label(frame, text="Ative este codigo no painel Keeply.", justify="left", anchor="w", bg="#ffffff", fg="#475569", font=(font_family, 12))
hint.pack(anchor="w", pady=(0, 14))
progress = ttk.Progressbar(frame, mode="indeterminate", style="Keeply.Horizontal.TProgressbar", length=520)
progress.pack(fill="x")
progress.start(12)
root.update_idletasks()
content_width = max(frame.winfo_reqwidth() + 36, 620)
info.configure(wraplength=content_width - 100)
hint.configure(wraplength=content_width - 100)
root.update_idletasks()
w = max(content_width, 620)
h = max(frame.winfo_reqheight() + 40, 400)
sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
root.geometry(f"{w}x{h}+{max((sw-w)//2,0)}+{max((sh-h)//2,0)}")
root.mainloop()
PY
    printf '%s\n' "${script_path}"
}
print_pairing_code_text(){
    local code="$1" device_name="$2" host_name="$3"
    echo
    echo "Codigo de ativacao Keeply: ${code}"
    echo "  dispositivo: ${device_name}"
    echo "  host       : ${host_name}"
}
show_pairing_popup(){
    local code="$1" device_name="$2" host_name="$3"
    [[ -n "${code}" ]] || return 0
    if ! command -v python3 >/dev/null 2>&1; then
        log_popup_fallback "python3 indisponivel"
        print_pairing_code_text "${code}" "${device_name}" "${host_name}"
        return 0
    fi
    local popup_user popup_uid popup_session popup_display popup_xauthority popup_dbus popup_wayland popup_runtime_dir popup_home popup_detail
    popup_user="$(find_graphical_user)"
    popup_uid=""
    [[ -z "${popup_user}" ]] || popup_uid="$(id -u "${popup_user}" 2>/dev/null || true)"
    popup_session="$(find_graphical_session_id "${popup_user}")"
    popup_display="$(find_display_value "${popup_user}" "${popup_uid}" "${popup_session}")"
    popup_wayland="$(find_wayland_display "${popup_user}" "${popup_session}")"
    popup_runtime_dir="$(find_xdg_runtime_dir "${popup_user}" "${popup_uid}" "${popup_session}")"
    popup_xauthority="$(find_xauthority_path "${popup_user}" "${popup_uid}" "${popup_session}")"
    popup_dbus="$(find_dbus_address "${popup_user}" "${popup_uid}" "${popup_session}" "${popup_runtime_dir}")"
    popup_home=""
    [[ -z "${popup_user}" ]] || popup_home="$(getent passwd "${popup_user}" | awk -F: '{print $6}' | head -n 1)"
    if [[ -z "${popup_display}" ]]; then
        log_popup_fallback "DISPLAY nao detectado" "user=${popup_user:-?} session=${popup_session:-?} wayland=${popup_wayland:-<vazio>}"
        print_pairing_code_text "${code}" "${device_name}" "${host_name}"
        return 0
    fi
    PAIRING_POPUP_SCRIPT="$(write_pairing_popup_script)"
    PAIRING_POPUP_LOG="$(mktemp /tmp/keeply-popup-log-XXXXXX.txt)"
    if [[ -n "${popup_user}" && -n "${popup_uid}" ]] && command -v runuser >/dev/null 2>&1; then
        setsid runuser -u "${popup_user}" -- env \
            DISPLAY="${popup_display}" \
            WAYLAND_DISPLAY="${popup_wayland}" \
            XAUTHORITY="${popup_xauthority}" \
            XDG_RUNTIME_DIR="${popup_runtime_dir}" \
            DBUS_SESSION_BUS_ADDRESS="${popup_dbus}" \
            HOME="${popup_home}" \
            python3 "${PAIRING_POPUP_SCRIPT}" "${code}" "${device_name}" "${host_name}" \
            >"${PAIRING_POPUP_LOG}" 2>&1 &
    else
        setsid env \
            DISPLAY="${popup_display}" \
            WAYLAND_DISPLAY="${popup_wayland}" \
            XAUTHORITY="${popup_xauthority}" \
            XDG_RUNTIME_DIR="${popup_runtime_dir}" \
            DBUS_SESSION_BUS_ADDRESS="${popup_dbus}" \
            HOME="${popup_home}" \
            python3 "${PAIRING_POPUP_SCRIPT}" "${code}" "${device_name}" "${host_name}" \
            >"${PAIRING_POPUP_LOG}" 2>&1 &
    fi
    PAIRING_POPUP_PID=$!
    sleep 1
    if ! kill -0 "${PAIRING_POPUP_PID}" >/dev/null 2>&1; then
        popup_detail="$(sed -n '1,8p' "${PAIRING_POPUP_LOG}" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g' | sed 's/^ //; s/ $//')"
        log_popup_fallback "popup morreu ao iniciar" "${popup_detail:-sem detalhe no log}"
        close_pairing_popup
        print_pairing_code_text "${code}" "${device_name}" "${host_name}"
    fi
}
wait_for_pairing_activation(){
    local meta_file="$1" timeout_secs="${2:-300}"
    SECONDS=0
    while (( SECONDS < timeout_secs )); do
        local device_id pairing_code
        device_id="$(read_device_id "${meta_file}" || true)"
        [[ -z "${device_id}" ]] || return 0
        pairing_code="$(read_pairing_code "${meta_file}" || true)"
        [[ -n "${pairing_code}" ]] || return 1
        sleep 2
    done
    return 2
}
PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATCH_ROOT="${1:-/}"
WS_URL="${2:-wss://backend.keeply.app.br/ws/agent}"
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
[[ "${WS_URL}" =~ ^wss?:// ]] || die "WS_URL invalido (esperado ws:// ou wss://): ${WS_URL}"
mkdir -p "${SYSTEMD_DIR}"
PROTECT_HOME_VALUE="yes"
case "${WATCH_ROOT}" in
    /|/home|/home/*|/root|/root/*) PROTECT_HOME_VALUE="no" ;;
esac
if ! getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
    log "criando grupo ${SERVICE_GROUP}"
    groupadd --force --system "${SERVICE_GROUP}"
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
for sig in TERM KILL; do
    pgrep -xf "${AGENT_BIN_DST}( .*)?" >/dev/null 2>&1 && pkill -"${sig}" -xf "${AGENT_BIN_DST}( .*)?" 2>/dev/null || true
    pgrep -xf "${CBT_BIN_DST}( .*)?" >/dev/null 2>&1 && pkill -"${sig}" -xf "${CBT_BIN_DST}( .*)?" 2>/dev/null || true
    [[ "${sig}" == "TERM" ]] && sleep 1
done
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${BIN_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${KEEPLY_HOME}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_DATA_HOME_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_STATE_HOME_DIR}"
install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${XDG_CACHE_HOME_DIR}"
install -d -m 700 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${KEEPLY_TMP_DIR}"
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
ProtectSystem=strict
ReadWritePaths=${KEEPLY_HOME}
ProtectHome=${PROTECT_HOME_VALUE}
PrivateTmp=yes
NoNewPrivileges=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
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
ProtectSystem=strict
ReadWritePaths=${KEEPLY_HOME} ${WATCH_ROOT}
ProtectHome=${PROTECT_HOME_VALUE}
PrivateTmp=yes
NoNewPrivileges=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
[Install]
WantedBy=multi-user.target
EOF2
else
    rm -f "${CBT_SERVICE_FILE}"
fi
mkdir -p /etc/default
cat > "${AGENT_ENV_FILE}" <<EOF2
KEEPLY_WS_URL=$(escape_systemd_env "${WS_URL}")
KEEPLY_ROOT=$(escape_systemd_env "${WATCH_ROOT}")
KEEPLY_DATA_DIR=$(escape_systemd_env "${KEEPLY_HOME}")
KEEPLY_DISABLE_POPUP=1
HOME=$(escape_systemd_env "${KEEPLY_HOME}")
XDG_DATA_HOME=$(escape_systemd_env "${XDG_DATA_HOME_DIR}")
XDG_STATE_HOME=$(escape_systemd_env "${XDG_STATE_HOME_DIR}")
XDG_CACHE_HOME=$(escape_systemd_env "${XDG_CACHE_HOME_DIR}")
TMPDIR=$(escape_systemd_env "${KEEPLY_TMP_DIR}")
EOF2
cat > "${CBT_ENV_FILE}" <<EOF2
KEEPLY_ROOT=$(escape_systemd_env "${WATCH_ROOT}")
HOME=$(escape_systemd_env "${KEEPLY_HOME}")
XDG_DATA_HOME=$(escape_systemd_env "${XDG_DATA_HOME_DIR}")
XDG_STATE_HOME=$(escape_systemd_env "${XDG_STATE_HOME_DIR}")
XDG_CACHE_HOME=$(escape_systemd_env "${XDG_CACHE_HOME_DIR}")
TMPDIR=$(escape_systemd_env "${KEEPLY_TMP_DIR}")
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
sleep 2
systemctl is-active --quiet keeply-agent.service || failed=1
if [[ "${INSTALL_CBT_SERVICE}" == "1" ]]; then
    systemctl is-active --quiet keeply-cbt-daemon.service || failed=1
fi
PAIRING_META_FILE="${KEEPLY_HOME}/agent_identity/identity.meta"
PAIRING_CODE=""
for _ in $(seq 1 20); do
    PAIRING_CODE="$(read_pairing_code "${PAIRING_META_FILE}" || true)"
    [[ -n "${PAIRING_CODE}" ]] && break
    sleep 1
done
show_pairing_popup "${PAIRING_CODE}" "${SERVICE_USER}" "$(hostname 2>/dev/null || echo keeply-host)"
if [[ -n "${PAIRING_CODE}" ]]; then
    if wait_for_pairing_activation "${PAIRING_META_FILE}" 300; then
        log "ativacao confirmada pelo backend."
    else
        log "timeout ou erro aguardando ativacao."
    fi
    close_pairing_popup
fi
log "instalacao concluida."
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
    payload_hash="$(sha256sum "${payload_tgz}" | awk '{print $1}')"
    cat > "${tmp_dir}/installer.sh" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ "${EUID}" -eq 0 ]] || { echo "Execute como root: sudo $0" >&2; exit 1; }
WATCH_ROOT="${1:-/}"
WS_URL="${2:-wss://backend.keeply.app.br/ws/agent}"
PREFIX="${3:-/opt/keeply}"
SYSTEMD_DIR="${4:-/etc/systemd/system}"
TMP_DIR="$(mktemp -d)"
cleanup(){ rm -rf "${TMP_DIR}"; }
trap cleanup EXIT
ARCHIVE_LINE=$((__ARCHIVE_LINE__ + 1))
EXPECTED_HASH="__PAYLOAD_HASH__"
payload_file="${TMP_DIR}/_payload.tgz"
tail -n +"${ARCHIVE_LINE}" "$0" > "${payload_file}"
if command -v sha256sum >/dev/null 2>&1; then
    actual_hash="$(sha256sum "${payload_file}" | awk '{print $1}')"
    if [[ "${actual_hash}" != "${EXPECTED_HASH}" ]]; then
        echo "Erro: integridade do payload falhou." >&2
        echo "  esperado: ${EXPECTED_HASH}" >&2
        echo "  obtido  : ${actual_hash}" >&2
        exit 1
    fi
fi
tar -xzf "${payload_file}" -C "${TMP_DIR}"
rm -f "${payload_file}"
exec "${TMP_DIR}/install_systemd_services.sh" "${WATCH_ROOT}" "${WS_URL}" "${PREFIX}" "${SYSTEMD_DIR}"
__ARCHIVE_BELOW__
EOF
    archive_line="$(wc -l < "${tmp_dir}/installer.sh")"
    sed -e "s/__ARCHIVE_LINE__/${archive_line}/" \
        -e "s/__PAYLOAD_HASH__/${payload_hash}/" \
        "${tmp_dir}/installer.sh" > "${out}"
    cat "${payload_tgz}" >> "${out}"
    chmod +x "${out}"
    installer_hash="$(sha256sum "${out}" | awk '{print $1}')"
    log "instalador gerado:"
    echo "  ${out}"
    echo "  installer sha256: ${installer_hash}"
    echo "  payload sha256  : ${payload_hash}"
}
verify_install() {
    local prefix="${1:-/opt/keeply}"
    local bin_dir="${prefix%/}/bin"
    local rc=0
    echo "Verificando instalacao Keeply..."
    for bin in keeply_agent keeply_cbt_daemon; do
        if [[ -x "${bin_dir}/${bin}" ]]; then
            echo "  [OK] ${bin_dir}/${bin}"
        else
            echo "  [FAIL] ${bin_dir}/${bin} nao encontrado ou nao executavel"
            rc=1
        fi
    done
    for svc in keeply-agent.service keeply-cbt-daemon.service; do
        if systemctl is-active --quiet "${svc}" 2>/dev/null; then
            echo "  [OK] ${svc} ativo"
        elif systemctl is-enabled --quiet "${svc}" 2>/dev/null; then
            echo "  [WARN] ${svc} habilitado mas inativo"
            rc=1
        else
            echo "  [--] ${svc} nao instalado"
        fi
    done
    return "${rc}"
}
cmd="${1:-}"
[[ -n "${cmd}" ]] || { usage; exit 1; }
shift || true
case "${cmd}" in
    build-agent) build_agent "$@" ;;
    build-installer) build_installer "$@" ;;
    verify) verify_install "$@" ;;
    *) usage; exit 1 ;;
esac
