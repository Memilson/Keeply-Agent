# Keeply Agent

Agente nativo em **C++17** responsável por varrer diretórios locais, produzir snapshots deduplicados e sincronizá-los com o backend web via **WebSocket + HTTP/TLS**. Roda como daemon (ou em foreground) no Linux, com lock de instância única e reconexão automática.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Tecnologias e Dependências](#tecnologias-e-dependências)
- [Arquitetura de Módulos](#arquitetura-de-módulos)
- [Formato do Repositório Local](#formato-do-repositório-local)
- [Processos Principais](#processos-principais)
  - [Backup Full](#backup-full)
  - [Backup Incremental (CBT)](#backup-incremental-cbt)
  - [Restore](#restore)
  - [Verificação de Integridade](#verificação-de-integridade)
  - [Upload para Cloud](#upload-para-cloud)
- [Segurança](#segurança)
- [CLI e Flags](#cli-e-flags)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Logs](#logs)
- [Protocolo WebSocket](#protocolo-websocket)
- [API REST Local](#api-rest-local)
- [Daemon CBT](#daemon-cbt)
- [CI/CD](#cicd)
- [Build e Instalação](#build-e-instalação)
- [Métricas do Código](#métricas-do-código)

---

## Visão Geral

O Keeply Agent é o componente cliente da plataforma Keeply. Ele:

1. **Varre** o sistema de arquivos local a partir de um diretório raiz configurável.
2. **Deduplica** blocos (chunks) globalmente via hash BLAKE3.
3. **Comprime** com zstd (nível 3) e opcionalmente **cifra** com AES-256-GCM.
4. **Persiste** snapshots imutáveis em formato SQLite (`.kipy`) + pack append-only (`.klyp`).
5. **Sincroniza** com o backend cloud via upload HTTP multipart com mTLS.
6. **Monitora** mudanças em tempo real via inotify (daemon CBT) para backups incrementais.
7. **Recebe comandos** do backend via WebSocket (backup, restore, listagem de FS).

---

## Tecnologias e Dependências

### Linguagem e Padrão

| Item | Detalhe |
|---|---|
| Linguagem | **C++17** (`std::filesystem`, `std::optional`, `if constexpr`) |
| Build System | **CMake ≥ 3.16** + Ninja/Make |
| Compiladores | GCC e Clang (Linux) |
| Plataforma alvo | **Linux** (x86_64, aarch64) |

### Dependências Externas

| Biblioteca | Papel | Obrigatória |
|---|---|---|
| **OpenSSL** (libssl + libcrypto) | TLS/SSL (WebSocket WSS, HTTP/TLS, mTLS), AES-256-GCM, SHA-256, geração de IV (`RAND_bytes`), certificados X.509 | ✅ |
| **SQLite3** | Banco de dados embarcado para snapshots (`.kipy`), estado CBT (`.cbtdb`), event store | ✅ |
| **zlib** | Compressão deflate (fallback/legado) | ✅ |
| **zstd** (libzstd) | Compressão primária de chunks (nível 3, ~3× mais rápido que zlib) | ✅ |
| **BLAKE3** (1.5.1, via FetchContent) | Hash criptográfico de chunks e arquivos (256-bit), compilado com SSE2/SSE4.1/AVX2 em x86_64 | ✅ (auto-baixado) |
| **librsync** | Assinaturas e deltas byte-level (rsync-style) para transferências incrementais | ❌ Opcional |
| **zenity** | Indicador de tray no desktop Linux | ❌ Opcional (runtime) |
| **systemd** | Gerenciamento do daemon CBT como serviço | ❌ Opcional (runtime) |

### Tecnologias e Protocolos Utilizados

| Tecnologia | Uso |
|---|---|
| **WebSocket (RFC 6455)** | Comunicação bidirecional persistente com o backend; reconexão exponencial com jitter (1s → 60s) |
| **HTTP/1.1 + TLS** | Upload de bundles (multipart), download de `.kply` anterior, registro de agente |
| **mTLS** | Autenticação mútua via certificado X.509 do agente (par chave/cert em `agent_identity/`) |
| **inotify** (Linux) | Monitoramento de filesystem em tempo real para Change Block Tracking (CBT) |
| **Content-Defined Chunking** | Divisão de arquivos em blocos de 8 MiB (`CHUNK_SIZE`) para deduplicação global |
| **flock(2)** | Lock de instância única (`LOCK_EX | LOCK_NB`) via PID file |
| **fork/exec + setsid** | Daemonização POSIX clássica (double-fork) |
| **XDG Base Directory** | Respeita `XDG_DATA_HOME`, `XDG_STATE_HOME`, `XDG_CACHE_HOME`, `user-dirs.dirs` |

---

## Arquitetura de Módulos

```
keeply.cpp                 ── bootstrap, parseArgs, loop WS, single-instance lock,
│                              detecção de hardware, daemonização, tray indicator
│
├── API/
│   ├── keeply_api.hpp      ── fachada estável (KeeplyApi): runBackup, restoreFile,
│   └── keeply_api.cpp         listSnapshots, diffSnapshots, setScanScope
│
├── Backup/
│   ├── varredura.cpp       ── ScanEngine::backupFolderToKply (full + CBT)
│   ├── restauracao.cpp     ── RestoreEngine (tmp+rename, verificação BLAKE3)
│   ├── verificacao.cpp     ── VerifyEngine (integridade SQLite + chunks)
│   ├── delta_rsync.cpp/hpp ── (opcional) assinaturas rsync para delta byte-level
│   ├── engines.hpp         ── interfaces ScanEngine, RestoreEngine, VerifyEngine
│   └── Rastreamento/
│       ├── rastreamento_mudancas.cpp/hpp ── EventStore, ChangeTracker,
│       │                                    CompositeChangeTracker,
│       │                                    BackgroundCbtWatcher
│       └── Linux/
│           └── daemon.cpp  ── daemon inotify (keeply_cbt_daemon / embutido)
│
├── Core/
│   ├── tipos.hpp           ── tipos fundamentais: ChunkHash, Blob, DB, Stmt,
│   │                          BackupStats, BackupProgress, ShardedChunkSet
│   ├── utilitarios.hpp     ── helpers: trim, hexEncode/Decode, parseTruthyValue
│   ├── compactacao.cpp     ── BLAKE3, zlib, zstd, AES-256-GCM (encrypt/decrypt/IV)
│   ├── plataforma.cpp      ── paths XDG, homeDir, exclusões de sistema,
│   │                          sqlite3_open_path, normalizeAbsolutePath
│   ├── tls_context.cpp/hpp ── criação de sessão TLS com SNI e mTLS
│   └── sqlite_util.hpp     ── helpers SQLite inline
│
├── Storage/
│   ├── arquivo_armazenamento.cpp/hpp ── StorageArchive (SQLite .kipy + pack .klyp),
│   │                                    hot prepared statements, RAII transactions
│   └── backend_armazenamento.hpp     ── StorageBackend abstrato, pack append-only
│
├── Cloud/
│   ├── fila_upload.cpp/hpp ── runParallelUploadQueue (N workers, retry c/ backoff)
│
├── HTTP/
│   ├── cliente_http.cpp    ── cliente HTTP/TLS puro (sem libcurl), socket direto,
│   │                          POST JSON, GET, upload multipart com streaming
│   ├── http_util.cpp/hpp   ── ParsedUrl, urlEncode, escapeJson, socket helpers
│   └── servidor_rest.cpp/hpp ── servidor REST embarcado com KeeplyRestApi
│                                (health, state, backup, restore, snapshots, diff)
│
├── WebSocket/
│   ├── websocket_agente.cpp/hpp ── KeeplyAgentWsClient: connect, run, sendText,
│   │                                handleServerMessage_, frame read/write
│   ├── websocket_interno.hpp    ── tipos internos WS, upload de bundle, helpers
│   ├── inicializacao.cpp        ── KeeplyAgentBootstrap: registro, pairing,
│   │                               geração de certificado X.509, persistência
│   ├── ws_cmd_backup.cpp        ── handler de backup via WS (com upload)
│   ├── ws_cmd_restore.cpp       ── handler de restore via WS (local + cloud)
│   └── ws_cmd_fs.cpp            ── handler de listagem FS (fs.list, fs.disks)
│
└── cmake/
    └── copy_runtime_deps.cmake  ── copia DLLs de runtime (cross-platform)
```

---

## Formato do Repositório Local

O agente separa estado **canônico** e **efêmero** em dois diretórios distintos:

- **`KEEPLY_DATA_DIR`** (default `~/.local/share/keeply`) — estado **canônico**: o arquivo `.kipy` (sqlite) + pack `.klyp`. É o que vai pra cloud.
- **`KEEPLY_STATE_DIR`** (default `~/.local/state/keeply`) — estado **efêmero**: bancos do CBT (`.cbtdb`). Podem ser apagados a qualquer momento; o pior caso é uma varredura full na próxima execução.

### Extensões

| Extensão | Onde vive | Papel |
|---|---|---|
| `.kipy` | `share/keeply/` | Arquivo principal (SQLite): `snapshots`, `files`, `file_chunks`, `chunks` dedupadas por BLAKE3, `meta`. Canônico — vai pra cloud junto com o pack. |
| `.klyp` (+ `.klyp.idx`) | `share/keeply/` | Pack append-only com chunks comprimidos (zstd) e opcionalmente cifrados (AES-GCM). Companheiro do `.kipy`. |
| `.cbtdb` | `state/keeply/` | Banco efêmero do CBT. **Não** vai pra cloud. Reconstrutível. |

### Arquivos Concretos

| Arquivo | Papel |
|---|---|
| `keeply.kipy` + `keeply.klyp` (+ `.klyp.idx`) | Repositório principal (canônico). |
| `keeply_cbt_state.cbtdb` | Estado do scanner CBT nativo (`cbt_state_roots`, `cbt_state_files` com `size/mtime`). |
| `keeply_cbt_events.cbtdb` | Log append-only de eventos do daemon CBT (`cbt_events` + `cbt_event_roots`). Limpo via `ackConsumedUpTo` após cada backup. |
| `keeply_agent.pid` | Lock (`flock`) — garante uma única instância. |
| `agent_identity/` | Chave privada, certificado X.509 do agente e metadados persistidos (deviceId, userId, fingerprint). |

Um **snapshot** é sempre um ponto completo: contém todos os arquivos visíveis naquele momento (os não modificados são clonados via `cloneFileFromPrevious`, reutilizando os chunks já existentes no pack). Não há replay de deltas — pra voltar à última versão basta restaurar o snapshot mais recente.

---

## Processos Principais

### Backup Full

`ScanEngine::backupFolderToKply` (`Backup/varredura.cpp`) executa:

1. Abre o `.kipy` e lê a flag `scan.cbt.enabled` da tabela `meta`.
2. `BEGIN` transação, cria um novo `snapshot` (ID autoincrement + label).
3. `discoverFiles()` percorre o root com `recursive_directory_iterator(skip_permission_denied)`, respeitando a política de exclusão (`isExcludedBySystemPolicy`).
4. Para cada arquivo regular:
   - Compara `(size, mtime)` com o mapa do snapshot anterior.
   - **Se bate** → `cloneFileFromPrevious` (zero bytes lidos).
   - **Se muda** → lê em chunks de `CHUNK_SIZE` (8 MiB), calcula BLAKE3, comprime com zstd (nível 3), cifra opcionalmente com AES-GCM (IV por chunk) se `KEEPLY_BACKUP_KEY` estiver setado, e chama `insertChunkIfMissing` (dedup global por hash).
5. Ao final, atualiza `file_hash` (BLAKE3 da sequência de chunk hashes) e, se `KEEPLY_HAVE_RSYNC`, salva a assinatura rsync do arquivo pra permitir delta futuro.
6. `COMMIT`. Qualquer exceção dispara `rollback` e o snapshot parcial é descartado.

O dedup é sempre global: mesmo rodando full, se um chunk já existe no pack ele não é regravado.

### Backup Incremental (CBT)

Ativado quando **(a)** `scan.cbt.enabled = 1` na `meta` do `.kipy` **e** **(b)** existe snapshot anterior.

O pipeline usa `CompositeChangeTracker` (`Backup/Rastreamento/rastreamento_mudancas.cpp`), que combina dois backends:

1. **`daemon_events`** — lê `cbt_events` do `keeply_cbt_events.cbtdb` alimentado pelo daemon inotify (`BackgroundCbtWatcher` ou `keeply_cbt_daemon` via systemd). Dá eventos quase em tempo real.
2. **`linux_state_sqlite`** (fallback) — re-scan do FS comparando contra `cbt_state_files` persistido. Não é "CBT verdadeiro", mas é determinístico.

O token retornado ao final é um `uint64` com 4 bits de backend + 60 bits de valor, persistido em `snapshots.cbt_token`. Na próxima execução, `decodeToken` decide qual backend consumir.

Fluxo em `discoverFilesFromTracker`:

- `prevMap` = mapa do snapshot anterior.
- `changes` = lista do tracker (`upsert` / `modify` / `delete`).
- Clona **todos os paths não mencionados** (reuso puro).
- Retorna apenas os paths `changed` pra releitura — `deleted` ficam de fora do novo snapshot.

Se o tracker lançar exceção durante `discoverFilesFromTracker`, o agente faz `rollback`, reseta o progresso e refaz a varredura como full.

#### Sobre CDP

O daemon capta eventos continuamente, mas **hoje não existe disparador automático de backup**. O `CbtEventLogger` em `keeply.cpp` apenas imprime os eventos. Para virar CDP real é preciso transformar o logger num coalescer que chame `runBackupCommand_` quando o buffer atingir N eventos ou T segundos.

### Restore

`RestoreEngine::restoreFile` / `restoreSnapshot` (`Backup/restauracao.cpp`):

1. Abre o `.kipy` em `beginRead`/`endRead` (scope RAII — `ArchiveReadSession`).
2. `buildSafeRestoreTarget` normaliza o relPath, proíbe paths absolutos ou com `..`, e confere que a resolução fica dentro do `outRoot`.
3. Escreve em `<target>.keeply.tmp`.
4. Concatena os chunks do pack, **recalculando BLAKE3 da sequência**; se divergir de `files.file_hash`, aborta — o temp é removido pelo RAII `TempFileCleanup`.
5. `std::filesystem::rename` atômico (com fallback de `remove + rename` se o destino já existir).
6. `applyRestoredMtime` restaura o mtime do snapshot.
7. `KEEPLY_RESTORE_FSYNC=1` força `fsync` antes e depois do rename.

O comando WS `restore.cloud.snapshot` baixa o bundle S3 completo (archive + index + blobs), remonta o pack se necessário e chama o mesmo fluxo local — ver `WebSocket/ws_cmd_restore.cpp`.

### Verificação de Integridade

`VerifyEngine::verifyArchive` (`Backup/verificacao.cpp`):

- Valida a integridade do banco SQLite e de todos os chunks armazenados.
- Verifica hashes BLAKE3 de cada chunk contra o hash registrado.
- Retorna `VerifyResult` com contadores de chunks verificados/ok e lista de erros.

### Upload para Cloud

1. `StorageArchive::prepareCloudUpload` gera um `CloudUploadPlan` com `bundleId`, referências aos chunks, paths do archive e pack.
2. `uploadArchiveBackup` (`WebSocket/websocket_interno.hpp`) faz upload multipart via HTTP/TLS com mTLS.
3. `runParallelUploadQueue` (`Cloud/fila_upload.cpp`) executa N workers em paralelo com retry exponencial (750ms backoff, até 3 tentativas).
4. Progresso reportado via WebSocket (`backup.upload.started|progress|uploaded|failed`).

---

## Segurança

| Mecanismo | Detalhe |
|---|---|
| **Criptografia at-rest** | AES-256-GCM por chunk, IV aleatório (`RAND_bytes`), chave derivada de `KEEPLY_BACKUP_KEY` (hex direto ou SHA-256 da string) |
| **Integridade** | BLAKE3 por chunk + `file_hash` = BLAKE3 da sequência de chunk hashes |
| **TLS** | OpenSSL com verificação de certificado (desabilitável com `KEEPLY_INSECURE_TLS` apenas para teste) |
| **mTLS** | Certificado X.509 auto-gerado persistido em `agent_identity/`, usado para autenticação com o backend |
| **Instância única** | `flock(LOCK_EX | LOCK_NB)` no PID file impede execuções concorrentes |
| **Path traversal** | `buildSafeRestoreTarget` proíbe `..` e paths absolutos no restore |
| **Permissões** | `umask(077)` no daemon; PID file criado com `0644` |
| **SIGPIPE** | Ignorado para evitar crash em writes em sockets fechados |

---

## CLI e Flags

```
keeply                                # agente WS em modo daemon
keeply --foreground                   # idem, sem daemonizar (stdout visível)
keeply --url wss://host/ws/agent
       --device "meu-pc"
       --root /home/user
       --no-tray --insecure-tls
keeply cbt --root /home/user --foreground   # roda só o daemon CBT
```

`SingleInstanceGuard` usa `flock(LOCK_EX|LOCK_NB)` em `keeply_agent.pid`; se outra instância estiver rodando, o segundo processo sai com erro.

---

## Variáveis de Ambiente

| Variável | Efeito |
|---|---|
| `KEEPLY_WS_URL` | URL WebSocket (default `wss://backend.keeply.app.br/ws/agent`). |
| `KEEPLY_DEVICE_NAME` | Nome exibido no console/painel. |
| `KEEPLY_DATA_DIR` | Override do diretório de dados do agente (`~/.local/share/keeply`). |
| `KEEPLY_ROOT` | Root padrão a ser varrido (default `$HOME`). |
| `KEEPLY_DISABLE_TRAY` | Desabilita o indicador `zenity`. |
| `KEEPLY_INSECURE_TLS` | Pula verificação TLS (apenas teste). |
| `KEEPLY_VERBOSE` | Logs extra no startup (OS, fingerprint SHA-256). |
| `KEEPLY_RESTORE_FSYNC` | Força `fsync` no restore. |
| `KEEPLY_CBT_DAEMON` | Caminho explícito do binário `keeply_cbt_daemon`. |
| `KEEPLY_BACKUP_KEY` | Chave para AES-256-GCM (64 hex chars = raw key; outro valor = SHA-256). |
| `KEEPLY_AGENT_PID_FILE` | Override do path do PID file. |

---

## Logs

Prefixos padronizados:

```
[backup] iniciado | label=... | source=...
[backup][fase] scan|compactacao|finalizacao|concluido|uploading
[backup] concluido | scanned=.. added=.. unchanged=.. bytes=.. warnings=..
[keeply][cbt] <label> | <relPath>
[keeply][cbt][warn] ...             (quando um backend CBT cai)
[keeply][tls][warn] verificacao TLS desabilitada por configuracao explicita
```

O agente nunca derruba o loop principal por falha de upload ou de CBT — ele continua online e volta a aguardar comandos.

---

## Protocolo WebSocket

O agente abre uma conexão WSS persistente com reconexão exponencial + jitter (1s → 60s). Comandos relevantes enviados pelo backend:

| Comando | Handler |
|---|---|
| `backup:<label>\|<storagePolicy>` | `runBackupCommand_` |
| `restore.file:<snapshot>\|<relPath>\|<outRoot>` | `runRestoreFileCommand_` |
| `restore.snapshot:<snapshot>\|<outRoot>` | `runRestoreSnapshotCommand_` |
| `restore.cloud.snapshot` (JSON) | `runRestoreCloudSnapshotCommand_` |
| `fs.list` / `fs.disks` | `ws_cmd_fs.cpp` |

Eventos enviados pelo agente: `agent.hello`, `backup.started|progress|finished|failed`, `backup.upload.started|progress|uploaded|failed`, `restore.*`, `state`.

### Endpoints HTTP Esperados no Backend

O agente também bate em dois endpoints HTTP (mesma origem do WS):

- `GET /api/agent/backups/latest-kply?userId&agentId&folderName&sourcePath` — devolve `{ url }` pré-assinado pro `.kply` anterior, usado como base pra incremental.
- `POST /api/agent/backups/upload` (multipart `file` + campos `userId,agentId,bundleId,bundleFileName,bundleRole,...`) — grava o bundle no S3 e persiste o row de `backups`.

> **Atenção:** na base atual do `Site/back/` os métodos (`StorageService.getLatestKplyUrl`, `StorageService.uploadAgentBackup`) existem mas **não há controller expondo essas rotas**. É preciso criar um `AgentBackupsController` em `api/agent/backups` antes de liberar o fluxo cloud.

---

## API REST Local

O agente embarca um servidor REST local (`HTTP/servidor_rest.cpp`) com os seguintes endpoints:

| Método | Endpoint | Função |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/state` | Estado atual do agente |
| `POST` | `/config/source` | Configurar diretório fonte |
| `POST` | `/config/archive` | Configurar path do archive |
| `POST` | `/config/restore-root` | Configurar diretório de restore |
| `POST` | `/backup` | Disparar backup com label |
| `GET` | `/backup/jobs` | Listar jobs de backup |
| `GET` | `/backup/jobs/:id` | Status de um job específico |
| `GET` | `/snapshots` | Listar snapshots |
| `GET` | `/diff` | Diff entre dois snapshots |
| `GET` | `/snapshots/:id/paths` | Listar arquivos de um snapshot |
| `POST` | `/restore/file` | Restaurar arquivo específico |
| `POST` | `/restore/snapshot` | Restaurar snapshot completo |

WebSocket hub (`KeeplyWsHub`) permite broadcast de eventos JSON e subscrição por job.

---

## Daemon CBT

`keeply.cpp` integra o código do daemon via `#include "Backup/Rastreamento/Linux/daemon.cpp"` e roteia `keeply cbt ...` pro `main` renomeado (`keeply_cbt_main`). Em produção o daemon é empacotado em binário separado (`keeply_cbt_daemon`) e iniciado por `systemd` (`keeply-cbt-daemon.service`). Se o systemd não estiver disponível, `ensureLinuxCbtDaemon` faz `fork+exec` do binário irmão; se nem isso der, cai pro `BackgroundCbtWatcher` in-process.

### Cascata de Fallback do CBT

```
1. systemd (keeply-cbt-daemon.service)
      ↓ falha
2. fork+exec do binário keeply_cbt_daemon
      ↓ não encontrado
3. BackgroundCbtWatcher in-process (inotify direto)
      ↓ falha
4. Sem CBT — backup full a cada execução
```

O daemon apenas grava eventos em `keeply_cbt_events.cbtdb`; a materialização em backup ainda depende de `runBackupCommand_` ser chamado (manualmente, via scheduler do plano, ou — no futuro — por um coalescer CDP). Após cada backup bem-sucedido, `ScanEngine::backupFolderToKply` chama `ackConsumedUpTo(newCbtToken)` e o EventStore poda os eventos já consumidos (mantendo o watermark `last_seq` pra não reusar sequências).

---

## CI/CD

O projeto usa **GitHub Actions** (`.github/workflows/build-installers.yml`):

- **Trigger**: push em `main`/`master`, pull requests, workflow_dispatch.
- **Runner**: `ubuntu-24.04`.
- **Processo**: instala dependências (`build-essential cmake ninja-build pkg-config libssl-dev zlib1g-dev libsqlite3-dev libzstd-dev`), roda `./keeply.sh build-installer` e produz `keeply-all.bin` (instalador Linux self-contained).
- **Artefato**: upload do `.bin` com retenção de 14 dias.

---

## Build e Instalação

### Dependências (Ubuntu/Debian)

```bash
sudo apt-get install -y \
    build-essential cmake ninja-build pkg-config \
    libssl-dev zlib1g-dev libsqlite3-dev libzstd-dev
# Opcional (para deltas rsync):
sudo apt-get install -y librsync-dev
```

### Compilação

```bash
mkdir -p build && cd build
cmake .. -G Ninja
ninja
```

Isso gera dois binários:

| Binário | Função |
|---|---|
| `keeply` | Agente principal (daemon WS + backup + restore + CBT embutido) |
| `keeply_cbt_daemon` | Daemon CBT standalone (mínimo: só SQLite + inotify) |

### Scripts de Desenvolvimento

| Script | Função |
|---|---|
| `restart.sh` | Mata processo anterior e reinicia em foreground com WS local |
| `apagar_pastas_restart.sh` | Apaga diretórios `~/.local/share/keeply` e `~/.local/state/keeply` (reset completo) |

---

## Métricas do Código

| Métrica | Valor |
|---|---|
| **Linhas de código C++** | ~7.473 |
| **Arquivos fonte** (.cpp/.hpp) | 30 |
| **Módulos** | 7 (Core, API, Backup, Storage, Cloud, HTTP, WebSocket) |
| **Schema SQLite** | Versão 9 |
| **Protocolo** | Versão 1 |
| **Chunk size** | 8 MiB |

### Top 5 Maiores Arquivos

| Arquivo | Linhas |
|---|---|
| `Storage/arquivo_armazenamento.cpp` | 762 |
| `HTTP/cliente_http.cpp` | 753 |
| `keeply.cpp` | 560 |
| `WebSocket/inicializacao.cpp` | 555 |
| `WebSocket/websocket_agente.cpp` | 546 |
