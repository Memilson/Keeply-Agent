#include "websocket_interno.hpp"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/x509.h>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <system_error>
#include <thread>
#include <utility>
#ifdef __linux__
#include <csignal>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
namespace keeply {
namespace {
using namespace ws_internal;
struct EvpPkeyDeleter {
    void operator()(EVP_PKEY* p) const noexcept { if (p) EVP_PKEY_free(p); }};
struct EvpPkeyCtxDeleter {
    void operator()(EVP_PKEY_CTX* p) const noexcept { if (p) EVP_PKEY_CTX_free(p); }};
struct X509Deleter {
    void operator()(X509* p) const noexcept { if (p) X509_free(p); }};
struct FileDeleter {
    void operator()(std::FILE* fp) const noexcept { if (fp) std::fclose(fp); }};
using UniquePkey    = std::unique_ptr<EVP_PKEY, EvpPkeyDeleter>;
using UniquePkeyCtx = std::unique_ptr<EVP_PKEY_CTX, EvpPkeyCtxDeleter>;
using UniqueX509    = std::unique_ptr<X509, X509Deleter>;
using UniqueFile    = std::unique_ptr<std::FILE, FileDeleter>;
namespace ui {
constexpr const char* kBrandBlue     = "#1E88E5";
constexpr const char* kBrandBlueDark = "#1565C0";
constexpr const char* kBrandWhite    = "#FFFFFF";
constexpr const char* kTextMuted     = "#64748B";
constexpr const char* kTextDark      = "#1E293B";
constexpr const char* kCodeBg        = "#EFF6FF";
constexpr const char* kBorderLight   = "#BFDBFE";
constexpr const char* kFallbackDevice = "Keeply Agent";
constexpr const char* kFallbackHost   = "desconhecido";
constexpr const char* kWindowTitle    = "Keeply";
constexpr int kWindowWidth  = 460;
constexpr int kWindowHeight = 340;
inline std::string buildHtmlPopup(const std::string& code,
                                  const std::string& deviceName,
                                  const std::string& hostName) {
    const std::string dev  = deviceName.empty() ? kFallbackDevice : deviceName;
    const std::string host = hostName.empty()   ? kFallbackHost   : hostName;
    std::string codeFormatted;
    for (std::size_t i = 0; i < code.size(); ++i) {
        if (i > 0 && i % 4 == 0) codeFormatted += ' ';
        codeFormatted += code[i];}
    std::ostringstream html;
    html << R"(<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Keeply</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
     background:)" << kBrandWhite << R"(;color:)" << kTextDark << R"(;
     display:flex;flex-direction:column;align-items:center;justify-content:center;
     min-height:100vh;padding:40px 24px;text-align:center}
.logo{display:flex;align-items:center;gap:10px;margin-bottom:28px}
.logo-dot{width:32px;height:32px;border-radius:8px;background:)" << kBrandBlue << R"(;
           display:flex;align-items:center;justify-content:center}
.logo-dot svg{width:18px;height:18px}
.logo-text{font-size:22px;font-weight:700;color:)" << kBrandBlueDark << R"(;letter-spacing:-0.5px}
.subtitle{font-size:13px;color:)" << kTextMuted << R"(;margin-bottom:24px;font-weight:500}
.code-box{background:)" << kCodeBg << R"(;border:2px solid )" << kBorderLight << R"(;
          border-radius:16px;padding:20px 36px;margin-bottom:24px}
.code{font-size:38px;font-weight:800;letter-spacing:6px;color:)" << kBrandBlueDark << R"(;
      font-family:'SF Mono',Monaco,Consolas,'Liberation Mono',monospace}
.info{font-size:12px;color:)" << kTextMuted << R"(;line-height:1.8}
.info strong{color:)" << kTextDark << R"(;font-weight:600}
.divider{width:48px;height:3px;border-radius:2px;background:)" << kBrandBlue << R"(;
         margin:20px auto;opacity:0.3}
.hint{font-size:12px;color:)" << kTextMuted << R"(;margin-top:8px;opacity:0.7}
</style></head><body>
<div class="logo">
  <div class="logo-dot">
    <svg viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5"
         stroke-linecap="round" stroke-linejoin="round">
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
    </svg>
  </div>
  <span class="logo-text">Keeply</span>
</div>
<p class="subtitle">C&oacute;digo de ativa&ccedil;&atilde;o do agente</p>
<div class="code-box">
  <div class="code">)" << codeFormatted << R"(</div>
</div>
<div class="info">
  <div><strong>Dispositivo:</strong> )" << dev << R"(</div>
  <div><strong>Host:</strong> )" << host << R"(</div>
</div>
<div class="divider"></div>
<p class="hint">Insira este c&oacute;digo no painel Keeply &mdash; mantenha esta janela aberta.</p>
</body></html>)";
    return html.str();}
inline std::string buildPlainMessage(const std::string& code,
                                     const std::string& deviceName,
                                     const std::string& hostName) {
    const std::string dev  = deviceName.empty() ? kFallbackDevice : deviceName;
    const std::string host = hostName.empty()   ? kFallbackHost   : hostName;
    std::ostringstream out;
    out << "Keeply \xe2\x80\x94 C\xc3\xb3" "digo de ativa\xc3\xa7\xc3\xa3o\n\n"
        << "  " << code << "\n\n"
        << "Dispositivo: " << dev << "\n"
        << "Host: " << host << "\n\n"
        << "Insira este c\xc3\xb3" "digo no painel Keeply.";
    return out.str();}}
struct PairingStatusResponse {
    std::string status;
    std::string deviceId;
    std::string code;
    std::string userId;
    bool isActive()  const noexcept { return status == "active"; }
    bool isExpired() const noexcept { return status == "expired" || status == "missing"; }
    bool isConflict() const noexcept { return status == "code_conflict"; }
};
class PairingPopupHandle {
public:
    PairingPopupHandle() = default;
    ~PairingPopupHandle() { close(); }
    PairingPopupHandle(const PairingPopupHandle&) = delete;
    PairingPopupHandle& operator=(const PairingPopupHandle&) = delete;
    void show(const std::string& code,
              const std::string& deviceName,
              const std::string& hostName){
        const std::string normCode   = trim(code);
        const std::string normDevice = trim(deviceName);
        const std::string normHost   = trim(hostName);
        if (normCode.empty()) return;
        if (normCode == shownCode_ && normDevice == shownDevice_ && normHost == shownHost_) return;
        close();
        shownCode_   = normCode;
        shownDevice_ = normDevice;
        shownHost_   = normHost;
        launchNativePopup(normCode, normDevice, normHost);}
    void close() {
        closeNativePopup();
        shownCode_.clear();
        shownDevice_.clear();
        shownHost_.clear();}
private:
    std::string shownCode_;
    std::string shownDevice_;
    std::string shownHost_;
    static std::string formatCode(const std::string& code) {
        std::string out;
        for (std::size_t i = 0; i < code.size(); ++i) {
            if (i > 0 && i % 4 == 0) out += ' ';
            out += code[i];}
        return out;}
#ifdef __linux__
    pid_t pid_ = -1;
    fs::path scriptPath_;
    static bool popupDisabled_() {
        const char* raw = std::getenv("KEEPLY_DISABLE_POPUP");
        if (!raw) return false;
        const std::string value = trim(raw);
        return value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES";}
    static bool hasGraphicalSession_() {
        const char* display = std::getenv("DISPLAY");
        if (display && *display) return true;
        const char* wayland = std::getenv("WAYLAND_DISPLAY");
        return wayland && *wayland;}
    static std::string buildTkPopupScript_() {
        return R"PY(
import sys
import tkinter as tk
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
font_family_bold = font_family
code_font_family = "DejaVu Sans Mono" if "DejaVu Sans Mono" in families else font_family
style = ttk.Style(root)
try:
    if "clam" in style.theme_names():
        style.theme_use("clam")
except Exception:
    pass
style.configure(
    "Keeply.Horizontal.TProgressbar",
    troughcolor="#dbeafe",
    background="#1e88e5",
    lightcolor="#1e88e5",
    darkcolor="#1565c0",
    bordercolor="#bfdbfe",
)
frame = tk.Frame(root, bg="#ffffff", padx=34, pady=28)
frame.pack(fill="both", expand=True)
title = tk.Label(
    frame,
    text="Keeply - Ativacao",
    bg="#ffffff",
    fg="#1565c0",
    font=(font_family_bold, 18, "bold"),
)
title.pack(pady=(0, 22))
subtitle = tk.Label(
    frame,
    text="Codigo de ativacao Keeply",
    bg="#ffffff",
    fg="#475569",
    font=(font_family, 12),
)
subtitle.pack(anchor="w")
code_box = tk.Frame(
    frame,
    bg="#eff6ff",
    highlightbackground="#bfdbfe",
    highlightcolor="#bfdbfe",
    highlightthickness=2,
    bd=0,
    padx=18,
    pady=14,
)
code_box.pack(fill="x", pady=(10, 18))
code_label = tk.Label(
    code_box,
    text=code,
    bg="#eff6ff",
    fg="#1565c0",
    font=(code_font_family, 19, "bold"),
)
code_label.pack(anchor="center")
info = tk.Label(
    frame,
    text=f"Dispositivo: {device}\nHost: {host}",
    justify="left",
    bg="#ffffff",
    fg="#334155",
    font=(font_family, 12),
)
info.pack(anchor="w", pady=(0, 18))
hint = tk.Label(
    frame,
    text="Ative este codigo no painel Keeply.",
    bg="#ffffff",
    fg="#475569",
    justify="left",
    anchor="w",
    font=(font_family, 12),
)
hint.pack(anchor="w", pady=(0, 14))
progress = ttk.Progressbar(
    frame,
    mode="indeterminate",
    style="Keeply.Horizontal.TProgressbar",
    length=520,
)
progress.pack(fill="x")
progress.start(12)
root.update_idletasks()
content_width = max(frame.winfo_reqwidth() + 36, 620)
info.configure(wraplength=content_width - 100)
hint.configure(wraplength=content_width - 100)
root.update_idletasks()
w = max(content_width, 620)
h = max(frame.winfo_reqheight() + 40, 400)
sw = root.winfo_screenwidth()
sh = root.winfo_screenheight()
x = max((sw - w) // 2, 0)
y = max((sh - h) // 2, 0)
root.geometry(f"{w}x{h}+{x}+{y}")
root.mainloop()
)PY";}
    static fs::path writeTkPopupScript_() {
        std::string templ = (fs::temp_directory_path() / "keeply-popup-XXXXXX.py").string();
        const int fd = ::mkstemps(templ.data(), 3);
        if (fd < 0) {
            throw std::runtime_error("Falha ao criar script temporario do popup.");}
        try {
            UniqueFile fp(::fdopen(fd, "wb"));
            if (!fp) {
                ::close(fd);
                throw std::runtime_error("Falha ao abrir script temporario do popup.");}
            const std::string script = buildTkPopupScript_();
            if (std::fwrite(script.data(), 1, script.size(), fp.get()) != script.size()) {
                throw std::runtime_error("Falha ao escrever script temporario do popup.");}
            return fs::path(templ);
        } catch (...) {
            std::error_code ec;
            fs::remove(fs::path(templ), ec);
            throw;}}
    void launchNativePopup(const std::string& code,
                           const std::string& device,
                           const std::string& host){
        if (popupDisabled_() || !hasGraphicalSession_()) return;
        try {
            scriptPath_ = writeTkPopupScript_();
        } catch (...) {
            scriptPath_.clear();
            return;}
        pid_ = fork();
        if (pid_ < 0) { pid_ = -1; return; }
        if (pid_ == 0) {
            ::execlp("python3",
                     "python3",
                     scriptPath_.string().c_str(),
                     code.c_str(),
                     device.c_str(),
                     host.c_str(),
                     static_cast<char*>(nullptr));
            _exit(127);}}
    void closeNativePopup() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status = 0;
            waitpid(pid_, &status, 0);
            pid_ = -1;}
        if (!scriptPath_.empty()) {
            std::error_code ec;
            fs::remove(scriptPath_, ec);
            scriptPath_.clear();}}
#endif
};
void tightenPermissions(const fs::path& path, bool executable) {
#ifdef __linux__
    const mode_t mode = executable ? mode_t{0700} : mode_t{0600};
    if (::chmod(path.c_str(), mode) != 0) {
        throw std::runtime_error("Falha ao ajustar permissoes de " + path.string());}
#else
    (void)path; (void)executable;
#endif
}
void ensureIdentityPermissions(const AgentIdentity& id) {
    if (fs::exists(id.certPemPath)) tightenPermissions(id.certPemPath, false);
    if (fs::exists(id.keyPemPath))  tightenPermissions(id.keyPemPath,  false);
    if (fs::exists(id.metaPath))    tightenPermissions(id.metaPath,    false);}
std::string generateDeviceId() {
    std::random_device rd;
    unsigned char bytes[16];
    for (auto& byte : bytes) byte = static_cast<unsigned char>(rd());
    return "dev_" + ws_internal::hexEncode(bytes, sizeof(bytes));}
std::string computeFingerprint(X509* cert) {
    const int derLen = i2d_X509(cert, nullptr);
    if (derLen <= 0) throw std::runtime_error("Falha ao serializar certificado DER.");
    std::vector<unsigned char> der(static_cast<std::size_t>(derLen));
    unsigned char* ptr = der.data();
    if (i2d_X509(cert, &ptr) != derLen) {
        throw std::runtime_error("Falha ao serializar certificado DER.");}
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(der.data(), der.size(), digest);
    return ws_internal::hexEncode(digest, sizeof(digest));}
std::string computeFingerprintFromFile(const fs::path& certPemPath) {
    UniqueFile fp(fopenPath(certPemPath, "rb"));
    if (!fp) throw std::runtime_error("Falha ao abrir certificado do agente.");
    UniqueX509 cert(PEM_read_X509(fp.get(), nullptr, nullptr, nullptr));
    if (!cert) throw std::runtime_error("Falha ao ler certificado PEM do agente.");
    return computeFingerprint(cert.get());}
AgentIdentity generateSelfSignedIdentity(const WsClientConfig& config) {
    AgentIdentity identity;
    identity.certPemPath = config.identityDir / "agent-cert.pem";
    identity.keyPemPath  = config.identityDir / "agent-key.pem";
    identity.metaPath    = config.identityDir / "identity.meta";
    std::error_code ec;
    fs::create_directories(config.identityDir, ec);
    if (ec) throw std::runtime_error("Falha ao criar diretorio de identidade: " + ec.message());
    tightenPermissions(config.identityDir, true);
    UniquePkeyCtx pkeyCtx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
    if (!pkeyCtx) throw std::runtime_error("Falha ao criar contexto da chave.");
    if (EVP_PKEY_keygen_init(pkeyCtx.get()) <= 0)                  throw std::runtime_error("Falha ao inicializar keygen.");
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(pkeyCtx.get(), 2048) <= 0) throw std::runtime_error("Falha ao configurar bits da chave.");
    EVP_PKEY* rawKey = nullptr;
    if (EVP_PKEY_keygen(pkeyCtx.get(), &rawKey) <= 0) throw std::runtime_error("Falha ao gerar chave privada.");
    UniquePkey pkey(rawKey);
    UniqueX509 cert(X509_new());
    if (!cert) throw std::runtime_error("Falha ao criar certificado X509.");
    X509_set_version(cert.get(), 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), static_cast<long>(std::time(nullptr)));
    X509_gmtime_adj(X509_get_notBefore(cert.get()), 0);
    X509_gmtime_adj(X509_get_notAfter(cert.get()), 60L * 60 * 24 * 365 * 5);
    X509_set_pubkey(cert.get(), pkey.get());
    auto* subjectName = X509_get_subject_name(cert.get());
    if (!subjectName) throw std::runtime_error("Falha ao obter subject do certificado.");
    X509_NAME_add_entry_by_txt(subjectName, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>(config.deviceName.c_str()),
                               -1, -1, 0);
    X509_set_issuer_name(cert.get(), subjectName);
    if (X509_sign(cert.get(), pkey.get(), EVP_sha256()) <= 0) {
        throw std::runtime_error("Falha ao assinar certificado do agente.");}{
        UniqueFile fp(fopenPath(identity.keyPemPath, "wb"));
        if (!fp) throw std::runtime_error("Falha ao criar chave PEM do agente.");
        if (PEM_write_PrivateKey(fp.get(), pkey.get(), nullptr, nullptr, 0, nullptr, nullptr) != 1) {
            throw std::runtime_error("Falha ao salvar chave PEM do agente.");}}
    tightenPermissions(identity.keyPemPath, false);{
        UniqueFile fp(fopenPath(identity.certPemPath, "wb"));
        if (!fp) throw std::runtime_error("Falha ao criar cert PEM do agente.");
        if (PEM_write_X509(fp.get(), cert.get()) != 1) {
            throw std::runtime_error("Falha ao salvar cert PEM do agente.");}}
    tightenPermissions(identity.certPemPath, false);
    identity.fingerprintSha256 = computeFingerprint(cert.get());
    return identity;}
PairingStatusResponse parsePairingResponse(const ws_internal::HttpResponse& resp) {
    PairingStatusResponse out;
    out.status   = trim(extractJsonStringField(resp.body, "status"));
    out.deviceId = trim(extractJsonStringField(resp.body, "deviceId"));
    out.code     = trim(extractJsonStringField(resp.body, "code"));
    out.userId   = trim(extractJsonStringField(resp.body, "userId"));
    return out;}
std::string buildPairingStartBody(const WsClientConfig& config, const AgentIdentity& identity, const std::string& code) {
    std::ostringstream json;
    json << "{\"code\":\""                  << escapeJson(code)
         << "\",\"deviceName\":\""          << escapeJson(config.deviceName)
         << "\",\"hostName\":\""            << escapeJson(config.hostName)
         << "\",\"os\":\""                  << escapeJson(config.osName)
         << "\",\"deviceId\":\""            << escapeJson(identity.deviceId)
         << "\",\"userId\":\""              << escapeJson(identity.userId)
         << "\",\"certFingerprintSha256\":\"" << escapeJson(identity.fingerprintSha256)
         << "\"}";
    return json.str();}
PairingStatusResponse startPairing(const WsClientConfig& config,
                                   const AgentIdentity& identity,
                                   const std::string& code){
    const std::string url  = httpUrlFromWsUrl(config.url, "/api/devices/pairing/start");
    const std::string body = buildPairingStartBody(config, identity, code);
    const auto resp = httpPostJson(url, body, std::nullopt, std::nullopt, config.allowInsecureTls);
    if (resp.status == 409) return {"code_conflict", "", "", ""};
    if (resp.status < 200 || resp.status >= 300) {
        throw std::runtime_error("Falha ao iniciar pareamento. HTTP " +
                                 std::to_string(resp.status) + " | body=" + resp.body);}
    return parsePairingResponse(resp);}
bool isDuplicateFingerprintError(const std::string& message) {
    return message.find("devices_cert_fingerprint_sha256_key") != std::string::npos ||
           message.find("cert_fingerprint_sha256") != std::string::npos;}
void removeIdentityArtifacts(const AgentIdentity& identity) {
    std::error_code ec;
    if (!identity.certPemPath.empty()) fs::remove(identity.certPemPath, ec);
    ec.clear();
    if (!identity.keyPemPath.empty()) fs::remove(identity.keyPemPath, ec);
    ec.clear();
    if (!identity.metaPath.empty()) fs::remove(identity.metaPath, ec);}
PairingStatusResponse pollPairingStatus(const WsClientConfig& config,
                                        const AgentIdentity& identity,
                                        const std::string& code){
    const std::string url = httpUrlFromWsUrl(config.url, "/api/devices/pairing/status");
    std::ostringstream json;
    json << "{\"code\":\"" << escapeJson(code)
         << "\",\"deviceId\":\"" << escapeJson(identity.deviceId)
         << "\",\"userId\":\"" << escapeJson(identity.userId)
         << "\",\"certFingerprintSha256\":\"" << escapeJson(identity.fingerprintSha256)
         << "\"}";
    const auto resp = httpPostJson(url, json.str(), std::nullopt, std::nullopt, config.allowInsecureTls);
    if (resp.status < 200 || resp.status >= 300) {
        throw std::runtime_error("Falha ao consultar status do pareamento. HTTP " +
                                 std::to_string(resp.status) + " | body=" + resp.body);}
    return parsePairingResponse(resp);}
AgentIdentity loadPersistedIdentity(const WsClientConfig& config) {
    AgentIdentity identity;
    identity.metaPath = config.identityDir / "identity.meta";
    const auto meta = ws_internal::loadIdentityMeta(identity.metaPath);
    auto get = [&](const char* key) -> std::string {
        auto it = meta.find(key);
        return (it != meta.end()) ? trim(it->second) : std::string{};
    };
    identity.deviceId           = get("device_id");
    identity.userId             = get("user_id");
    identity.fingerprintSha256  = get("fingerprint_sha256");
    identity.pairingCode        = get("pairing_code");
    const std::string certMeta = get("cert_pem");
    const std::string keyMeta  = get("key_pem");
    identity.certPemPath = certMeta.empty() ? (config.identityDir / "agent-cert.pem") : pathFromUtf8(certMeta);
    identity.keyPemPath  = keyMeta.empty()  ? (config.identityDir / "agent-key.pem")  : pathFromUtf8(keyMeta);
    if (identity.deviceId.empty()) identity.deviceId = generateDeviceId();
    return identity;}
bool awaitPairingConfirmation(const WsClientConfig& config,
                              AgentIdentity& identity){
    const auto interval = std::chrono::milliseconds(std::max(1000, config.pairingPollIntervalMs));
    for (;;) {
        std::this_thread::sleep_for(interval);
        auto status = pollPairingStatus(config, identity, identity.pairingCode);
        if (status.isActive()) {
            identity.deviceId    = status.deviceId;
            identity.userId      = status.userId;
            identity.pairingCode.clear();
            return true;}
        if (status.isExpired()) {
            identity.pairingCode.clear();
            return false;}}}}
AgentIdentity KeeplyAgentBootstrap::ensureRegistered(const WsClientConfig& config) {
    if (trim(config.url).empty()) {
        throw std::runtime_error("URL do backend websocket nao pode ser vazia.");}
    fs::create_directories(config.identityDir);
    tightenPermissions(config.identityDir, true);
    PairingPopupHandle popup;
    AgentIdentity identity = loadPersistedIdentity(config);
    if (!fs::exists(identity.certPemPath) || !fs::exists(identity.keyPemPath)) {
        identity = generateSelfSignedIdentity(config);
    } else if (identity.fingerprintSha256.empty()) {
        identity.fingerprintSha256 = computeFingerprintFromFile(identity.certPemPath);}
    ensureIdentityPermissions(identity);
    if(!identity.userId.empty()){
        const auto persisted = pollPairingStatus(config, identity, "");
        if (persisted.isActive()) {
            if (!persisted.deviceId.empty()) identity.deviceId = persisted.deviceId;
            if (!persisted.userId.empty()) identity.userId = persisted.userId;
            identity.pairingCode.clear();
            ws_internal::saveIdentityMeta(identity);
            tightenPermissions(identity.metaPath, false);
            return identity;}
        const std::string configuredPairingCode = trim(config.pairingCode);
        identity.userId.clear();
        identity.pairingCode = configuredPairingCode.empty() ? ws_internal::randomDigits(8) : configuredPairingCode;
        std::cerr << "Dispositivo persistido nao encontrado no backend para deviceId=" << identity.deviceId
                  << ". Gerando novo codigo de ativacao.\n";
    }
    for (;;) {
        if (identity.pairingCode.empty()) identity.pairingCode = trim(config.pairingCode);
        if (identity.pairingCode.empty()) identity.pairingCode = ws_internal::randomDigits(8);
        PairingStatusResponse started;
        try {
            started = startPairing(config, identity, identity.pairingCode);
        } catch (const std::exception& ex) {
            if (isDuplicateFingerprintError(ex.what())) {
                removeIdentityArtifacts(identity);
                identity = generateSelfSignedIdentity(config);
                ensureIdentityPermissions(identity);
                identity.pairingCode.clear();
                continue;}
            throw;}
        if (started.isConflict()) {
            identity.pairingCode.clear();
            continue;}
        if (!started.code.empty()) identity.pairingCode = started.code;
        if (started.isActive()) {
            identity.deviceId = started.deviceId;
            identity.userId   = started.userId;
            identity.pairingCode.clear();
            break;}
        ws_internal::saveIdentityMeta(identity);
        popup.show(identity.pairingCode, config.deviceName, config.hostName);
        std::cout << "\n"
                  << "  \xe2\x96\x88 Keeply \xe2\x80\x94 C\xc3\xb3" "digo de ativa\xc3\xa7\xc3\xa3o\n"
                  << "  \xe2\x94\x82\n"
                  << "  \xe2\x94\x82  " << identity.pairingCode << "\n"
                  << "  \xe2\x94\x82\n"
                  << "  \xe2\x94\x82  Dispositivo: " << config.deviceName << "\n"
                  << "  \xe2\x94\x82  Host:         " << config.hostName << "\n"
                  << "  \xe2\x94\x94\xe2\x94\x80 Insira este c\xc3\xb3" "digo no painel Keeply.\n\n";
        if (awaitPairingConfirmation(config, identity)) break;}
    popup.close();
    ws_internal::saveIdentityMeta(identity);
    tightenPermissions(identity.metaPath, false);
    return identity;}
AgentIdentity KeeplyAgentBootstrap::loadPersistedIdentity(const WsClientConfig& config) {
    return ::keeply::loadPersistedIdentity(config);}}
