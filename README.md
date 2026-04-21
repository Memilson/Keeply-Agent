# Keeply Agent

Agente nativo em C++ responsável por varrer diretórios locais, produzir snapshots deduplicados em `.kply` e sincronizá-los com o backend web (WebSocket + HTTP).

## Sumário

- [Arquitetura](#arquitetura)
- [Formato do repositório local](#formato-do-repositório-local)
- [Backup full](#backup-full)
- [Backup incremental (CBT)](#backup-incremental-cbt)
- [Restore](#restore)
- [CLI e flags](#cli-e-flags)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Logs](#logs)
- [Protocolo WebSocket](#protocolo-websocket)
- [Daemon CBT](#daemon-cbt)

## Arquitetura

```
keeply.cpp            ── bootstrap, parseArgs, loop WS, single-instance lock
API/                  ── fachada estável (KeeplyApi): runBackup, restoreFile, ...
Backup/
  varredura.cpp       ── ScanEngine::backupFolderToKply (full + CBT)
  restauracao.cpp     ── RestoreEngine (tmp+rename, verificação BLAKE3)
  verificacao.cpp     ── VerifyEngine (integridade SQLite + chunks)
  delta_rsync.cpp     ── (opcional) assinaturas rsync para delta
  Rastreamento/       ── EventStore + ChangeTracker (daemon + state tracker)
Core/                 ── compactação (zstd/zlib/AES-GCM), BLAKE3, utilitários
Storage/              ── StorageArchive (SQLite .kply + pack .klyp)
Cloud/                ── fila de upload (backend HTTP)
HTTP/                 ── cliente HTTP/TLS, upload multipart, URL builder
..WebSocket/            ── cliente WS, comandos backup / restore / fs
GUI/                  ── janela GTK3 para testes locais de backup e restore
```

## Formato do repositório local

O agente separa estado canônico e efêmero em dois diretórios distintos:

- **`KEEPLY_DATA_DIR`** (default `~/.local/share/keeply`) — estado **canônico**: o arquivo `.kipy` (sqlite do arquivo de backup) + pack `.klyp`. É o que vai pra cloud.
- **`KEEPLY_STATE_DIR`** (default `~/.local/state/keeply`) — estado **efêmero**: bancos do CBT (`.cbtdb`). Podem ser apagados a qualquer momento; o pior caso é uma varredura full na próxima execução.

Convenção de extensões:

| Extensão | Onde vive | Papel |
|---|---|---|
| `.kipy` | `share/keeply/` | Arquivo principal (sqlite): `snapshots`, `files`, `file_chunks`, `chunks` dedupadas por BLAKE3, `meta`. Canônico — vai pra cloud junto com o pack. |
| `.klyp` (+ `.klyp.idx`) | `share/keeply/` | Pack append-only com chunks comprimidos (zstd) e opcionalmente cifrados (AES-GCM). Companheiro do `.kipy`. |
| `.cbtdb` | `state/keeply/` | Banco efêmero do CBT. **Não** vai pra cloud. Reconstrutível. |

Arquivos concretos:

| Arquivo | Papel |
|---|---|
| `keeply.kipy` + `keeply.klyp` (+ `.klyp.idx`) | Repositório principal (canônico). |
| `keeply_cbt_state.cbtdb` | Estado do scanner CBT nativo (`cbt_state_roots`, `cbt_state_files` com `size/mtime`). |
| `keeply_cbt_events.cbtdb` | Log append-only de eventos do daemon CBT (`cbt_events` + `cbt_event_roots`). Limpo via `ackConsumedUpTo` após cada backup. |
| `keeply_agent.pid` | Lock (`flock`) — garante uma única instância. |
| `agent_identity/` | Chave/cert do agente e fingerprint persistida. |

Um **snapshot** é sempre um ponto completo: contém todos os arquivos visíveis naquele momento (os não modificados são clonados via `cloneFileFromPrevious`, reutilizando os chunks já existentes no pack). Não há replay de deltas — pra voltar à última versão basta restaurar o snapshot mais recente.

## Backup full

`ScanEngine::backupFolderToKply` (`Backup/varredura.cpp`) executa:

1. Abre o `.kply` e lê a flag `scan.cbt.enabled` da tabela `meta`.
2. `BEGIN` transação, cria um novo `snapshot` (ID autoincrement + label).
3. `discoverFiles()` percorre o root com `recursive_directory_iterator(skip_permission_denied)`, respeitando a política de exclusão (`isExcludedBySystemPolicy`).
4. Para cada arquivo regular:
   - Compara `(size, mtime)` com o mapa do snapshot anterior.
   - **Se bate** → `cloneFileFromPrevious` (zero bytes lidos).
   - **Se muda** → lê em chunks de `CHUNK_SIZE`, calcula BLAKE3, comprime com zstd (nível 3), cifra opcionalmente com AES-GCM (IV por chunk) se `KEEPLY_KEY_*` estiver setado, e chama `insertChunkIfMissing` (dedup global por hash).
5. Ao final, atualiza `file_hash` (BLAKE3 da sequência de chunk hashes) e, se `KEEPLY_HAVE_RSYNC`, salva a assinatura rsync do arquivo pra permitir delta futuro.
6. `COMMIT`. Qualquer exceção dispara `rollback` e o snapshot parcial é descartado.

O dedup é sempre global: mesmo rodando full, se um chunk já existe no pack ele não é regravado.

## Backup incremental (CBT)

Ativado quando **(a)** `scan.cbt.enabled = 1` na `meta` do `.kply` **e** **(b)** existe snapshot anterior.

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

### Sobre CDP

O daemon capta eventos continuamente, mas **hoje não existe disparador automático de backup**. O `CbtEventLogger` em `keeply.cpp` apenas imprime os eventos. Para virar CDP real é preciso transformar o logger num coalescer que chame `runBackupCommand_` quando o buffer atingir N eventos ou T segundos.

## Restore

`RestoreEngine::restoreFile` / `restoreSnapshot` (`Backup/restauracao.cpp`):

1. Abre o `.kply` em `beginRead`/`endRead` (scope RAII — `ArchiveReadSession`).
2. `buildSafeRestoreTarget` normaliza o relPath, proíbe paths absolutos ou com `..`, e confere que a resolução fica dentro do `outRoot`.
3. Escreve em `<target>.keeply.tmp`.
4. Concatena os chunks do pack, **recalculando BLAKE3 da sequência**; se divergir de `files.file_hash`, aborta — o temp é removido pelo RAII `TempFileCleanup`.
5. `std::filesystem::rename` atômico (com fallback de `remove + rename` se o destino já existir).
6. `applyRestoredMtime` restaura o mtime do snapshot.
7. `KEEPLY_RESTORE_FSYNC=1` força `fsync` antes e depois do rename.

O comando WS `restore.cloud.snapshot` baixa o bundle S3 completo (archive + index + blobs), remonta o pack se necessário e chama o mesmo fluxo local — ver `WebSocket/ws_cmd_restore.cpp`.

## CLI e flags

```
keeply                                # agente WS em modo daemon
keeply --foreground                   # idem, sem daemonizar (stdout visível)
keeply --url wss://host/ws/agent
       --device "meu-pc"
       --root /home/user
       --no-tray --insecure-tls
keeply cbt --root /home/user --foreground   # roda só o daemon CBT
keeply_gui                            # janela GTK local para testar backup/restore
```

O alvo `keeply_gui` usa `KeeplyApi` direto, sem WebSocket nem frontend web. Ele existe apenas para testes locais de:

- escolher origem do backup
- escolher arquivo `.kipy`
- escolher pasta de restore
- disparar backup manual
- listar snapshots
- ver arquivos do snapshot
- restaurar snapshot inteiro
- restaurar um arquivo especifico

Para compilar esse binário, o ambiente precisa ter os headers de desenvolvimento do GTK3:

```bash
sudo apt install pkg-config libgtk-3-dev
```

`SingleInstanceGuard` usa `flock(LOCK_EX|LOCK_NB)` em `keeply_agent.pid`; se outra instância estiver rodando, o segundo processo sai com erro.

## Variáveis de ambiente

| Variável | Efeito |
|---|---|
| `KEEPLY_WS_URL` | URL WebSocket (default `wss://backend.keeply.app.br/ws/agent`). |
| `KEEPLY_DEVICE_NAME` | Nome exibido no console/painel. |
| `KEEPLY_DATA_DIR` | Override do diretório de dados do agente. |
| `KEEPLY_ROOT` | Root padrão a ser varrido. |
| `KEEPLY_DISABLE_TRAY` | Desabilita o indicador `zenity`. |
| `KEEPLY_INSECURE_TLS` | Pula verificação TLS (apenas teste). |
| `KEEPLY_VERBOSE` | Logs extra no startup. |
| `KEEPLY_RESTORE_FSYNC` | Força `fsync` no restore. |
| `KEEPLY_CBT_DAEMON` | Caminho explícito do binário `keeply_cbt_daemon`. |
| `KEEPLY_KEY_*` | Chave derivada para AES-GCM (ver `Compactador::deriveKeyFromEnv`). |

## Logs

Prefixos padronizados:

- `[backup] iniciado | label=... | source=...`
- `[backup][fase] scan|compactacao|finalizacao|concluido|uploading`
- `[backup] concluido | scanned=.. added=.. unchanged=.. bytes=.. warnings=..`
- `[keeply][cbt] <label> | <relPath>`
- `[keeply][cbt][warn] ...` quando um backend CBT cai
- `[keeply][tls][warn] verificacao TLS desabilitada por configuracao explicita`

O agente nunca derruba o loop principal por falha de upload ou de CBT — ele continua online e volta a aguardar comandos.

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

### Endpoints HTTP esperados no backend

O agente também bate em dois endpoints HTTP (mesma origem do WS):

- `GET /api/agent/backups/latest-kply?userId&agentId&folderName&sourcePath` — devolve `{ url }` pré-assinado pro `.kply` anterior, usado como base pra incremental.
- `POST /api/agent/backups/upload` (multipart `file` + campos `userId,agentId,bundleId,bundleFileName,bundleRole,...`) — grava o bundle no S3 e persiste o row de `backups`.

> **Atenção:** na base atual do `Site/back/` os métodos (`StorageService.getLatestKplyUrl`, `StorageService.uploadAgentBackup`) existem mas **não há controller expondo essas rotas**. É preciso criar um `AgentBackupsController` em `api/agent/backups` antes de liberar o fluxo cloud.

## Daemon CBT

`keeply.cpp` integra o código do daemon via `#include "Backup/Rastreamento/Linux/daemon.cpp"` e roteia `keeply cbt ...` pro `main` renomeado (`keeply_cbt_main`). Em produção o daemon é empacotado em binário separado (`keeply_cbt_daemon`) e iniciado por `systemd` (`keeply-cbt-daemon.service`). Se o systemd não estiver disponível, `ensureLinuxCbtDaemon` faz `fork+exec` do binário irmão; se nem isso der, cai pro `BackgroundCbtWatcher` in-process.

O daemon apenas grava eventos em `keeply_cbt_events.cbtdb`; a materialização em backup ainda depende de `runBackupCommand_` ser chamado (manualmente, via scheduler do plano, ou — no futuro — por um coalescer CDP). Após cada backup bem-sucedido, `ScanEngine::backupFolderToKply` chama `ackConsumedUpTo(newCbtToken)` e o EventStore poda os eventos já consumidos (mantendo o watermark `last_seq` pra não reusar sequências).
