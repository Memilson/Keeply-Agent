# Keeply Agent Mobile — Features Reference
## 1. Conexão WebSocket
### URL padrão
`wss://backend.keeply.app.br/ws/agent`
### Parâmetro de query
Ao conectar, adicionar `?deviceId=<agentId>` na URL.
### Handshake
- HTTP Upgrade com headers: `Upgrade: websocket`, `Connection: Upgrade`, `Sec-WebSocket-Key`, `Sec-WebSocket-Version: 13`, `User-Agent: keeply-agent/1.0`
- Validar resposta 101 + `Sec-WebSocket-Accept` (SHA1 da key + GUID RFC 6455)
### Keep-Alive
- Enviar ping frame (opcode 0x9) com payload `keepalive` a cada 25s
- Se pong não chegar em 15s, considerar conexão perdida
### Reconexão
- Intervalo base: 5000ms com jitter ±10%
- Loop infinito de reconexão
### Rate Limit
- Máximo 60 mensagens de texto por janela de 1 minuto
- Mensagens excedentes são descartadas silenciosamente
### Protocolo v1
- Toda mensagem JSON enviada pelo agente recebe campo `"v":1` no início do objeto
- Formato: `{"v":1,"type":"...","campo":"valor"}`
## 2. Identidade e Pairing
### Gerar Identidade
1. Gerar par RSA 2048 bits
2. Criar certificado X509 self-signed (CN = deviceName, validade 5 anos)
3. Salvar `agent-cert.pem` e `agent-key.pem` no diretório de identidade
4. Calcular fingerprint SHA-256 do certificado DER
5. Gerar `deviceId` = `"dev_"` + 16 bytes aleatórios em hex (32 chars)
### Pairing Flow
1. Gerar código de 8 dígitos aleatórios
2. `POST /api/devices/pairing/start` com body:
```json
{
  "code": "12345678",
  "deviceName": "nome-device",
  "hostName": "hostname",
  "os": "android",
  "deviceId": "dev_abc123...",
  "userId": "",
  "certFingerprintSha256": "hex..."
}
```
3. Se resposta status 409, gerar novo código e repetir
4. Poll `POST /api/devices/pairing/status` com body:
```json
{
  "code": "12345678",
  "deviceId": "dev_abc123...",
  "userId": "",
  "certFingerprintSha256": "hex..."
}
```
5. Respostas possíveis:
   - `"status":"active"` → pareamento concluído, salvar deviceId e userId
   - `"status":"expired"` ou `"status":"missing"` → gerar novo código
6. Intervalo de poll: 3000ms
### Persistência
Arquivo `identity.meta` com formato key=value:
```
device_id=dev_abc123...
user_id=usr_xyz789...
fingerprint_sha256=hex...
pairing_code=
cert_pem=path/to/agent-cert.pem
key_pem=path/to/agent-key.pem
```
## 3. Mensagem agent.hello
Enviada imediatamente após handshake WebSocket:
```json
{
  "type": "agent.hello",
  "protocolVersion": 1,
  "deviceId": "dev_...",
  "agentId": "dev_...",
  "osName": "android",
  "hostName": "pixel-7",
  "fingerprintSha256": "hex...",
  "connectedAt": "2024-01-15 10:30:00",
  "source": "/storage/emulated/0",
  "archive": "/data/data/br.app.keeply/keeply.kipy",
  "restoreRoot": "/data/data/br.app.keeply/restore",
  "deviceDetails": {
    "localIps": ["192.168.1.100"],
    "hardware": {
      "cpuModel": "Snapdragon 8 Gen 2",
      "cpuArchitecture": "aarch64",
      "kernelVersion": "5.15.78",
      "cpuCores": 8,
      "totalMemoryBytes": 8589934592
    }
  },
  "scanScope": {
    "id": "home",
    "label": "Home",
    "requestedPath": "",
    "resolvedPath": "/storage/emulated/0"
  }
}
```
## 4. Comandos WebSocket — Recebidos do Servidor
### 4.1 ping
Payload: `"ping"` ou `{"type":"ping"}`
Resposta: `{"type":"pong"}`
### 4.2 state
Payload: `"state"` ou `{"type":"state"}`
Resposta: JSON com type=state, deviceId, agentId, source, archive, restoreRoot, deviceDetails, scanScope, archiveSplitEnabled, archiveSplitMaxBytes
### 4.3 snapshots
Payload: `"snapshots"` ou `{"type":"snapshots"}`
Resposta:
```json
{
  "type": "snapshots",
  "deviceId": "dev_...",
  "agentId": "dev_...",
  "items": [
    {"id": 1, "createdAt": "2024-01-15 10:00:00", "sourceRoot": "/home/user", "label": "daily", "fileCount": 1500}
  ]
}
```
### 4.4 fs.list
Payload: `{"type":"fs.list","requestId":"req123","path":"/storage/emulated/0/Documents"}`
Resposta: JSON com type=fs.list, requestId, path, parentPath, truncated, items (array de {name, path, kind, size})
Máximo 120 items, apenas diretórios.
### 4.5 fs.disks
Payload: `{"type":"fs.disks","requestId":"req123"}`
Resposta: JSON com type=fs.disks, requestId, items (array de {name, path, kind, size})
### 4.6 scan.scope
Payload: `{"type":"scan.scope","scopeId":"documents"}`
Valores válidos de scopeId: home, documents, desktop, downloads, pictures, music, videos
Resposta: `{"type":"scan.scope.updated","agentId":"...","source":"...","scanScope":{...}}`
### 4.7 config.source
Payload: `{"type":"config.source","path":"/storage/emulated/0/DCIM"}`
Resposta: `{"type":"config.updated","field":"source","agentId":"...","source":"...","scanScope":{...}}`
### 4.8 config.archive
Payload: `{"type":"config.archive","path":"/data/data/br.app.keeply/keeply.kipy"}`
Resposta: `{"type":"config.updated","field":"archive"}`
### 4.9 config.restoreRoot
Payload: `{"type":"config.restoreRoot","path":"/storage/emulated/0/KeeplyRestore"}`
Resposta: `{"type":"config.updated","field":"restoreRoot"}`
### 4.10 backup
Payload: `{"type":"backup","label":"daily","storage":"cloud_only","source":"/path","key":"hex_or_passphrase"}`
Campos opcionais: label, storage, source, key
Eventos emitidos:
- `backup.started` — início do backup
- `backup.progress` — progresso (phase: discovery|backup|commit|done, currentFile, filesQueued, filesCompleted, filesScanned, filesAdded, filesUnchanged, chunksNew, bytesRead, warnings)
- `backup.finished` — conclusão (label, source, scanScope, filesScanned, filesAdded, filesUnchanged, chunksNew, chunksReused, bytesRead, warnings)
- `backup.failed` — falha (message + últimos stats)
- `backup.upload.started` — início do upload cloud
- `backup.upload.progress` — progresso upload (currentObject, bundleRole, filesUploaded, filesTotal, blobPartIndex, blobPartCount)
- `backup.uploaded` — upload concluído (storageId, storageScope, bucket, uri, bundleId, filesUploaded, blobPartCount)
- `backup.upload.failed` — falha no upload (message)
### 4.11 restore.file
Payload: `{"type":"restore.file","snapshot":"1","relPath":"Documents/file.txt","outRoot":"/restore","key":"..."}`
Eventos: restore.file.started, restore.file.finished, restore.file.failed
### 4.12 restore.snapshot
Payload: `{"type":"restore.snapshot","snapshot":"1","outRoot":"/restore","key":"..."}`
Eventos: restore.snapshot.started, restore.snapshot.finished, restore.snapshot.failed
### 4.13 restore.cloud.snapshot
Payload:
```json
{
  "type": "restore.cloud.snapshot",
  "snapshot": "1",
  "downloadPathBase": "/api/bundles/files",
  "archiveFile": "keeply.kipy",
  "backupId": "bk_...",
  "backupRef": "ref_...",
  "bundleId": "bd_...",
  "outRoot": "/restore",
  "relPath": "Documents/",
  "entryType": "folder",
  "key": "..."
}
```
Eventos: restore.cloud.started, restore.cloud.progress (phase: downloading|downloaded|assembling|restoring), restore.cloud.finished, restore.cloud.failed
## 5. Scan de Arquivos (Backup)
### Full Scan
1. Percorrer recursivamente o diretório source (skip_permission_denied)
2. Para cada arquivo regular, registrar path relativo
3. Excluir paths do sistema (/proc, /sys, /dev, /tmp, etc.)
4. Emitir progresso a cada 200 arquivos ou 1.2s
### Incremental (CBT)
1. Ler lastCbtToken do snapshot anterior
2. Consultar EventStore para mudanças desde lastToken
3. Arquivos não alterados: clonar do snapshot anterior (reuse)
4. Arquivos novos/modificados: processar normalmente
5. Arquivos deletados: não incluir no novo snapshot
### Processamento de Arquivo
- Arquivo < 1MB: acumular em bundle até 7MB, compactar bundle inteiro como chunk único
- Arquivo >= 1MB: dividir em chunks de 8MB cada
- Para cada chunk: BLAKE3 hash → zstd compress (level 3) → opcionalmente AES-256-GCM encrypt → inserir no SQLite se hash novo
- Hash do arquivo inteiro: BLAKE3 sobre todo conteúdo
## 6. Storage / Arquivo SQLite (.kipy)
### Schema v9
Tabelas principais:
- `snapshots` (id, created_at, source_root, label, backup_type, file_count, cbt_token)
- `files` (id, snapshot_id, rel_path, size, mtime, file_hash)
- `chunks` (chunk_hash BLOB PRIMARY KEY, raw_size, comp_size, comp_algo, pack_offset, encrypt_iv)
- `file_chunks` (file_id, chunk_idx, chunk_hash, raw_size, offset_in_chunk)
- `meta` (k TEXT PRIMARY KEY, v TEXT)
### Pack File (.klyp)
- Magic: `KPCHUNK1` (8 bytes)
- Records sequenciais, cada um com header:
  - Magic: `KPREC001` (8 bytes)
  - rawSize (uint64), compSize (uint64), hash (32 bytes), algoLen (uint32), version (uint32)
  - Algo string (algoLen bytes)
  - Compressed data (compSize bytes)
### Constantes
- CHUNK_SIZE = 8MB
- BUNDLE_FILE_THRESHOLD = 1MB
- BUNDLE_FLUSH_TARGET = 7MB
## 7. Restore
### Restore de arquivo único
1. Buscar file por snapshotId + relPath no SQLite
2. Carregar chunks associados (file_chunks JOIN chunks)
3. Para cada chunk: ler do pack → descomprimir (zstd/zlib) → opcionalmente decrypt AES-GCM
4. Concatenar chunks na ordem → escrever no diretório de destino
5. Verificar integridade BLAKE3 do arquivo reconstruído vs hash armazenado
6. Aplicar mtime original
### Restore de snapshot completo
1. Listar todos os paths do snapshot
2. Restaurar cada arquivo individualmente
### Restore Cloud
1. Baixar archiveFile do backend via HTTP GET
2. Se .zst, descomprimir zstd (header: rawSize uint64 + payload)
3. Abrir SQLite do archive baixado
4. Identificar chunks necessários para o snapshot
5. Baixar cada chunk individualmente via HTTP GET
6. Montar pack local com chunks baixados
7. Restaurar normalmente do pack montado
## 8. Criptografia
### AES-256-GCM (opcional, ativada por chave)
- Chave: variável de ambiente KEEPLY_BACKUP_KEY ou campo `key` do comando WS
- Se hex 64 chars: usar diretamente como 32 bytes
- Senão: SHA-256 da string para derivar 32 bytes
- IV: 12 bytes aleatórios (RAND_bytes)
- Formato encrypted: [16 bytes GCM tag] + [ciphertext]
- IV armazenado na coluna `encrypt_iv` da tabela chunks
### Hashing
- Algoritmo: BLAKE3 (32 bytes digest)
- Usado para: hash de chunks, hash de arquivos inteiros
### Compressão
- Primário: zstd (level 3)
- Fallback: zlib
- Raw: sem compressão (campo comp_algo = "raw")
## 9. Upload Cloud
### Bundle Export
1. Preparar CloudUploadPlan: archiveDbPath, packPath, latestSnapshotId, chunks com offsets
2. Upload via HTTP multipart para backend
### Upload Paralelo
- Default 2 workers
- Máximo 3 retries por task
- Backoff entre retries: 750ms × attempt
### Endpoints
- `POST /api/agent/backups/upload` — upload multipart do bundle
- `GET /api/agent/backups/latest-kply?userId=&agentId=&sourcePath=` — buscar base incremental
## 10. Escopos de Scan
| scopeId | Label | Resolução Android |
|---|---|---|
| home | Home | /storage/emulated/0 |
| documents | Documents | /storage/emulated/0/Documents |
| desktop | Desktop | N/A (fallback home) |
| downloads | Downloads | /storage/emulated/0/Download |
| pictures | Pictures | /storage/emulated/0/Pictures |
| music | Music | /storage/emulated/0/Music |
| videos | Videos | /storage/emulated/0/Movies |
