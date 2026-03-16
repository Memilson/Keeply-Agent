#include "keeply.hpp"
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_set>
#include <vector>
namespace {
using keeply::KeeplyApi;
using keeply::SnapshotRow;
constexpr std::size_t kFilePageSize = 25;
struct BrowserEntry {
    bool isDir{};
    std::string name;
    std::string relPath;
};
int cmdRestoreSnapshot(KeeplyApi& api, const std::string& snapshot);
int cmdRestoreFile(KeeplyApi& api, const std::string& relPath, const std::string& snapshot);
std::string readLine(const std::string& prompt) {
    std::cout << prompt;
    std::cout.flush();
    std::string s;
    std::getline(std::cin, s);
    return keeply::trim(s);
}
std::optional<std::size_t> parseIndex1(const std::string& s, std::size_t max) {
    if (s.empty()) return std::nullopt;
    try {
        const long long v = std::stoll(s);
        if (v < 1) return std::nullopt;
        const std::size_t idx = static_cast<std::size_t>(v);
        if (idx > max) return std::nullopt;
        return idx - 1;
    } catch (...) {
        return std::nullopt;
    }
}
std::string snapshotTypeLabel(std::size_t idx, std::size_t total) {
    if (total == 0) return "-";
    if (total == 1) return "Completo (atual)";
    if (idx == 0) return "Completo (base)";
    if (idx + 1 == total) return "Incremental (atual)";
    return "Incremental";
}
std::vector<SnapshotRow> loadTimelineSnapshots(KeeplyApi& api) {
    auto snapshots = api.listSnapshots();
    std::reverse(snapshots.begin(), snapshots.end());
    return snapshots;
}
void printConfig(const KeeplyApi& api) {
    const auto& st = api.state();
    std::cout << "Origem (scan): " << st.source << "\n";
    std::cout << "Arquivo backup: " << st.archive << "  [DB com chunks compactados]\n";
    std::cout << "Restore root  : " << st.restoreRoot << "\n";
    std::cout << "Split archive : "
              << (st.archiveSplitEnabled ? "ON" : "OFF");
    if (st.archiveSplitEnabled) {
        std::cout << " (maxBytes=" << st.archiveSplitMaxBytes << ")";
    }
    std::cout << "\n";
}
void printUsage(const char* argv0) {
    std::cout
        << "Keeply CLI (basico)\n\n"
        << "Uso:\n"
        << "  " << argv0 << "                 # menu interativo\n"
        << "  " << argv0 << " menu\n"
        << "  " << argv0 << " config\n"
        << "  " << argv0 << " backup\n"
        << "  " << argv0 << " backup-source <diretorio> [arquivo_backup]\n"
        << "  " << argv0 << " timeline\n"
        << "  " << argv0 << " reset [--force]\n"
        << "  " << argv0 << " restore-ui\n"
        << "  " << argv0 << " restore [snapshot]\n"
        << "  " << argv0 << " files [snapshot]\n"
        << "  " << argv0 << " restore-file <arquivo_relativo> [snapshot]\n\n"
        << "Defaults:\n"
        << "  - Scan origem: pasta Home do usuario\n"
        << "  - Backup DB   : " << keeply::pathToUtf8(keeply::defaultArchivePath()) << "\n"
        << "  - Restore root: " << keeply::pathToUtf8(keeply::defaultRestoreRootPath()) << "\n";
}
int cmdConfig(KeeplyApi& api) {
    printConfig(api);
    return 0;
}
int cmdBackup(KeeplyApi& api) {
    printConfig(api);
    std::cout << "Executando backup da origem '" << api.state().source << "' (pode demorar)...\n";
    const keeply::BackupStats stats = api.runBackup("");
    std::cout
        << "Backup concluido"
        << " | scanned=" << stats.scanned
        << " added=" << stats.added
        << " reused=" << stats.reused
        << " chunks=" << stats.chunks
        << " uniq_chunks=" << stats.uniqueChunksInserted
        << " bytes=" << stats.bytesRead
        << " warnings=" << stats.warnings
        << "\n";
    return 0;
}
int cmdBackupSource(KeeplyApi& api, const std::string& source, const std::optional<std::string>& archivePath) {
    api.setSource(source);
    if (archivePath && !archivePath->empty()) api.setArchive(*archivePath);
    return cmdBackup(api);
}
int cmdTimeline(KeeplyApi& api) {
    const auto snapshots = loadTimelineSnapshots(api);
    if (snapshots.empty()) {
        std::cout << "Historico vazio.\n";
        return 0;
    }
    std::cout << "Historico de backups (" << snapshots.size() << ")\n";
    std::cout << std::left
              << std::setw(5)  << "N"
              << std::setw(6)  << "ID"
              << std::setw(22) << "Criado em"
              << std::setw(22) << "Tipo"
              << std::setw(10) << "Arquivos"
              << "Origem\n";
    for (std::size_t i = 0; i < snapshots.size(); ++i) {
        const auto& s = snapshots[i];
        std::cout << std::left
                  << std::setw(5)  << (i + 1)
                  << std::setw(6)  << s.id
                  << std::setw(22) << s.createdAt
                  << std::setw(22) << snapshotTypeLabel(i, snapshots.size())
                  << std::setw(10) << s.fileCount
                  << s.sourceRoot
                  << "\n";
    }
    return 0;
}
int cmdReset(KeeplyApi& api, bool force) {
    const auto& st = api.state();
    const std::string root = keeply::pathToUtf8(keeply::defaultKeeplyDataDir());
    if (!force) {
        std::cout << "ATENCAO: isso vai apagar TUDO em " << root << "\n";
        std::cout << "Inclui backup DB e restauracoes de teste.\n";
        const std::string confirm = readLine("Digite APAGAR para confirmar: ");
        if (confirm != "APAGAR") {
            std::cout << "Cancelado.\n";
            return 0;
        }
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    if (ec) {
        throw std::runtime_error("Falha ao apagar " + root + ": " + ec.message());
    }
    keeply::ensureDefaults();
    std::cout << "Reset concluido.\n";
    std::cout << "Backup DB   : " << st.archive << "\n";
    std::cout << "Restore root: " << st.restoreRoot << "\n";
    return 0;
}
std::optional<std::string> chooseSnapshotInteractive(KeeplyApi& api) {
    const auto snapshots = loadTimelineSnapshots(api);
    if (snapshots.empty()) {
        std::cout << "Nenhum snapshot disponivel.\n";
        return std::nullopt;
    }
    std::cout << "\nSnapshots (linha do tempo)\n";
    for (std::size_t i = 0; i < snapshots.size(); ++i) {
        const auto& s = snapshots[i];
        std::cout << "  [" << (i + 1) << "] "
                  << "#" << s.id
                  << " | " << s.createdAt
                  << " | " << snapshotTypeLabel(i, snapshots.size())
                  << " | arquivos=" << s.fileCount
                  << "\n";
    }
    for (;;) {
        const std::string in = readLine("Selecione o snapshot pelo numero (0 cancela): ");
        if (in == "0" || in == "q" || in == "Q") return std::nullopt;
        const auto idx = parseIndex1(in, snapshots.size());
        if (!idx) {
            std::cout << "Numero invalido.\n";
            continue;
        }
        return std::to_string(static_cast<long long>(snapshots[*idx].id));
    }
}
std::optional<std::string> chooseFileInteractive(const std::vector<std::string>& paths) {
    if (paths.empty()) {
        std::cout << "Esse snapshot nao tem arquivos.\n";
        return std::nullopt;
    }
    std::size_t page = 0;
    const std::size_t pages = (paths.size() + kFilePageSize - 1) / kFilePageSize;
    for (;;) {
        const std::size_t start = page * kFilePageSize;
        const std::size_t end = std::min(paths.size(), start + kFilePageSize);
        std::cout << "\nArquivos do snapshot (pagina " << (page + 1) << "/" << pages << ")\n";
        for (std::size_t i = start; i < end; ++i) {
            std::cout << "  [" << (i + 1) << "] " << paths[i] << "\n";
        }
        std::cout << "Comandos: numero=selecionar | n=proxima | p=anterior | q=cancelar\n";
        const std::string in = readLine("> ");
        if (in == "q" || in == "Q" || in == "0") return std::nullopt;
        if (in == "n" || in == "N") {
            if (page + 1 < pages) ++page;
            continue;
        }
        if (in == "p" || in == "P") {
            if (page > 0) --page;
            continue;
        }
        const auto idx = parseIndex1(in, paths.size());
        if (!idx) {
            std::cout << "Entrada invalida.\n";
            continue;
        }
        return paths[*idx];
    }
}
bool hasPathPrefix(const std::string& path, const std::string& prefix) {
    if (prefix.empty()) return true;
    return path.size() >= prefix.size() && path.compare(0, prefix.size(), prefix) == 0;
}
std::string parentPrefix(const std::string& prefix) {
    if (prefix.empty()) return {};
    std::string p = prefix;
    if (!p.empty() && p.back() == '/') p.pop_back();
    const auto pos = p.find_last_of('/');
    if (pos == std::string::npos) return {};
    return p.substr(0, pos + 1);
}
std::vector<BrowserEntry> listBrowserEntries(const std::vector<std::string>& paths, const std::string& prefix) {
    std::vector<BrowserEntry> dirs;
    std::vector<BrowserEntry> files;
    std::unordered_set<std::string> seenDirs;
    for (const auto& path : paths) {
        if (!hasPathPrefix(path, prefix)) continue;
        const std::string rest = path.substr(prefix.size());
        if (rest.empty()) continue;
        const auto slash = rest.find('/');
        if (slash == std::string::npos) {
            files.push_back(BrowserEntry{false, rest, path});
            continue;
        }
        const std::string dirName = rest.substr(0, slash);
        if (!seenDirs.insert(dirName).second) continue;
        dirs.push_back(BrowserEntry{true, dirName, prefix + dirName});
    }
    std::sort(dirs.begin(), dirs.end(), [](const BrowserEntry& a, const BrowserEntry& b) { return a.name < b.name; });
    std::sort(files.begin(), files.end(), [](const BrowserEntry& a, const BrowserEntry& b) { return a.name < b.name; });
    std::vector<BrowserEntry> out;
    out.reserve(dirs.size() + files.size());
    out.insert(out.end(), dirs.begin(), dirs.end());
    out.insert(out.end(), files.begin(), files.end());
    return out;
}
std::size_t countPathsInDir(const std::vector<std::string>& paths, const std::string& dirPrefix) {
    std::size_t n = 0;
    for (const auto& p : paths) {
        if (hasPathPrefix(p, dirPrefix)) ++n;
    }
    return n;
}
int restorePathsMatchingPrefix(KeeplyApi& api,
                               const std::vector<std::string>& paths,
                               const std::string& snapshot,
                               const std::string& dirPrefix) {
    std::size_t total = 0;
    for (const auto& p : paths) {
        if (hasPathPrefix(p, dirPrefix)) ++total;
    }
    if (total == 0) {
        std::cout << "Nenhum arquivo encontrado para restore.\n";
        return 0;
    }
    std::cout << "Restaurando " << total << " arquivo(s) de '" << (dirPrefix.empty() ? "/" : dirPrefix) << "'...\n";
    std::size_t done = 0;
    for (const auto& p : paths) {
        if (!hasPathPrefix(p, dirPrefix)) continue;
        api.restoreFile(snapshot, p, std::nullopt);
        ++done;
        if (done % 100 == 0 || done == total) {
            std::cout << "RESTORE " << done << "/" << total << " path=" << p << "\n";
        }
    }
    std::cout << "Restore concluido em: " << api.state().restoreRoot << "\n";
    return 0;
}
int cmdRestoreBrowse(KeeplyApi& api, const std::string& snapshot) {
    const auto paths = api.listSnapshotPaths(snapshot);
    if (paths.empty()) {
        std::cout << "Esse snapshot nao tem arquivos.\n";
        return 0;
    }
    std::string prefix;
    std::size_t page = 0;
    for (;;) {
        const auto entries = listBrowserEntries(paths, prefix);
        const std::size_t pages = std::max<std::size_t>(1, (entries.size() + kFilePageSize - 1) / kFilePageSize);
        if (page >= pages) page = pages - 1;
        const std::size_t start = page * kFilePageSize;
        const std::size_t end = std::min(entries.size(), start + kFilePageSize);
        std::cout << "\nNavegando snapshot #" << snapshot << " em /" << prefix
                  << " (itens=" << entries.size()
                  << ", arquivos_no_dir=" << countPathsInDir(paths, prefix)
                  << ", pagina " << (page + 1) << "/" << pages << ")\n";
        if (entries.empty()) {
            std::cout << "  (vazio)\n";
        } else {
            for (std::size_t i = start; i < end; ++i) {
                const auto& e = entries[i];
                std::cout << "  [" << (i + 1) << "] " << (e.isDir ? "[D] " : "[F] ") << e.name;
                if (e.isDir) {
                    const std::string dirPrefix = e.relPath + "/";
                    std::cout << " (" << countPathsInDir(paths, dirPrefix) << " arqs)";
                }
                std::cout << "\n";
            }
        }
        std::cout << "Comandos: numero=entrar/restaurar | a=restaurar pasta atual | u=subir | r=raiz | n/p=pagina | q=sair\n";
        const std::string in = readLine("> ");
        if (in == "q" || in == "Q" || in == "0") return 0;
        if (in == "n" || in == "N") {
            if (page + 1 < pages) ++page;
            continue;
        }
        if (in == "p" || in == "P") {
            if (page > 0) --page;
            continue;
        }
        if (in == "u" || in == "U") {
            prefix = parentPrefix(prefix);
            page = 0;
            continue;
        }
        if (in == "r" || in == "R") {
            prefix.clear();
            page = 0;
            continue;
        }
        if (in == "a" || in == "A") {
            if (prefix.empty()) {
                const std::string confirm = readLine("Restaurar snapshot completo? (digite RESTAURAR): ");
                if (confirm == "RESTAURAR") return cmdRestoreSnapshot(api, snapshot);
                std::cout << "Cancelado.\n";
                continue;
            }
            const std::string confirm = readLine("Restaurar pasta atual '" + prefix + "'? (s/N): ");
            if (confirm == "s" || confirm == "S") {
                return restorePathsMatchingPrefix(api, paths, snapshot, prefix);
            }
            std::cout << "Cancelado.\n";
            continue;
        }
        const auto idx = parseIndex1(in, entries.size());
        if (!idx) {
            std::cout << "Entrada invalida.\n";
            continue;
        }
        const auto& e = entries[*idx];
        if (e.isDir) {
            std::cout << "  [1] Entrar na pasta\n";
            std::cout << "  [2] Restaurar essa pasta (recursivo)\n";
            std::cout << "  [0] Voltar\n";
            const std::string op = readLine("Opcao: ");
            if (op == "1") {
                prefix = e.relPath + "/";
                page = 0;
                continue;
            }
            if (op == "2") {
                const std::string dirPrefix = e.relPath + "/";
                const std::string confirm = readLine("Restaurar '" + dirPrefix + "'? (s/N): ");
                if (confirm == "s" || confirm == "S") {
                    return restorePathsMatchingPrefix(api, paths, snapshot, dirPrefix);
                }
                std::cout << "Cancelado.\n";
                continue;
            }
            continue;
        }
        return cmdRestoreFile(api, e.relPath, snapshot);
    }
}
int cmdRestoreSnapshot(KeeplyApi& api, const std::string& snapshot) {
    api.restoreSnapshot(snapshot, std::nullopt);
    std::cout << "Snapshot restaurado em: " << api.state().restoreRoot << "\n";
    return 0;
}
int cmdFiles(KeeplyApi& api, const std::string& snapshot) {
    const auto paths = api.listSnapshotPaths(snapshot);
    std::cout << "Arquivos no snapshot '" << snapshot << "': " << paths.size() << "\n";
    for (std::size_t i = 0; i < paths.size(); ++i) {
        std::cout << "  [" << (i + 1) << "] " << paths[i] << "\n";
    }
    return 0;
}
int cmdRestoreFile(KeeplyApi& api, const std::string& relPath, const std::string& snapshot) {
    api.restoreFile(snapshot, relPath, std::nullopt);
    const auto target = std::filesystem::path(api.state().restoreRoot) / relPath;
    std::cout << "Arquivo restaurado em: " << target.string() << "\n";
    return 0;
}
int cmdRestoreUi(KeeplyApi& api) {
    const auto snapshot = chooseSnapshotInteractive(api);
    if (!snapshot) {
        std::cout << "Restore cancelado.\n";
        return 0;
    }
    for (;;) {
        std::cout << "\nRestore do snapshot #" << *snapshot << "\n";
        std::cout << "  [1] Restaurar snapshot completo\n";
        std::cout << "  [2] Navegar e restaurar (arquivo/pasta)\n";
        std::cout << "  [0] Cancelar\n";
        const std::string mode = readLine("Opcao: ");
        if (mode == "0") {
            std::cout << "Restore cancelado.\n";
            return 0;
        }
        if (mode == "1") {
            return cmdRestoreSnapshot(api, *snapshot);
        }
        if (mode == "2") {
            return cmdRestoreBrowse(api, *snapshot);
        }
        std::cout << "Opcao invalida.\n";
    }
}
int runMenu(KeeplyApi& api) {
    for (;;) {
        std::cout << "\n=== KEEPLY CLI (BASICO) ===\n";
        printConfig(api);
        std::cout << "\n";
        std::cout << "  [1] Executar backup (scan em " << api.state().source << ")\n";
        std::cout << "  [2] Ver historico (completo/incremental)\n";
        std::cout << "  [3] Restaurar (selecionar snapshot e arquivo)\n";
        std::cout << "  [4] Apagar tudo em " << keeply::pathToUtf8(keeply::defaultKeeplyDataDir()) << " (teste)\n";
        std::cout << "  [5] Listar arquivos do ultimo snapshot\n";
        std::cout << "  [0] Sair\n";
        const std::string op = readLine("Opcao: ");
        if (op == "0") return 0;
        if (op == "1") {
            try { cmdBackup(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "2") {
            try { cmdTimeline(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "3") {
            try { cmdRestoreUi(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "4") {
            try { cmdReset(api, false); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "5") {
            try { cmdFiles(api, "latest"); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        std::cout << "Opcao invalida.\n";
    }
}
}
int main(int argc, char* argv[]) {
    try {
        KeeplyApi api;
        api.setScanScope("home");
        api.setArchive(keeply::pathToUtf8(keeply::defaultArchivePath()));
        api.setRestoreRoot(keeply::pathToUtf8(keeply::defaultRestoreRootPath()));
        if (argc < 2) {
            return runMenu(api);
        }
        const std::string cmd = argv[1];
        if (cmd == "help" || cmd == "-h" || cmd == "--help") {
            printUsage(argv[0]);
            return 0;
        }
        if (cmd == "menu") return runMenu(api);
        if (cmd == "config") return cmdConfig(api);
        if (cmd == "backup") return cmdBackup(api);
        if (cmd == "backup-source") {
            if (argc < 3) throw std::runtime_error("Uso: backup-source <diretorio> [arquivo_backup]");
            const std::optional<std::string> archive = (argc >= 4) ? std::optional<std::string>(argv[3]) : std::nullopt;
            return cmdBackupSource(api, argv[2], archive);
        }
        if (cmd == "timeline" || cmd == "list") return cmdTimeline(api);
        if (cmd == "restore-ui") return cmdRestoreUi(api);
        if (cmd == "restore-browse") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdRestoreBrowse(api, snapshot);
        }
        if (cmd == "reset") {
            bool force = (argc >= 3 && std::string(argv[2]) == "--force");
            return cmdReset(api, force);
        }
        if (cmd == "restore" || cmd == "restore-snapshot") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdRestoreSnapshot(api, snapshot);
        }
        if (cmd == "files" || cmd == "paths") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdFiles(api, snapshot);
        }
        if (cmd == "restore-file") {
            if (argc < 3) throw std::runtime_error("Uso: restore-file <arquivo_relativo> [snapshot]");
            const std::string relPath = argv[2];
            const std::string snapshot = (argc >= 4) ? argv[3] : "latest";
            return cmdRestoreFile(api, relPath, snapshot);
        }
        throw std::runtime_error("Comando invalido: " + cmd);
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << "\n";
        return 1;
    }
}
