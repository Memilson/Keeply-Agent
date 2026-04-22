class WsCommand {
  final String type;
  final String requestId;
  final String path;
  final String sourcePath;
  final String scopeId;
  final String label;
  final String storage;
  final String snapshot;
  final String relPath;
  final String outRoot;
  final String ticketId;
  final String downloadPathBase;
  final String backupId;
  final String backupRef;
  final String bundleId;
  final String archiveFile;
  final String indexFile;
  final String packFile;
  final String blobFiles;
  final String sourceRoot;
  final String entryType;
  final String encryptionKey;
  final String raw;
  WsCommand({
    this.type = '',
    this.requestId = '',
    this.path = '',
    this.sourcePath = '',
    this.scopeId = '',
    this.label = '',
    this.storage = '',
    this.snapshot = '',
    this.relPath = '',
    this.outRoot = '',
    this.ticketId = '',
    this.downloadPathBase = '',
    this.backupId = '',
    this.backupRef = '',
    this.bundleId = '',
    this.archiveFile = '',
    this.indexFile = '',
    this.packFile = '',
    this.blobFiles = '',
    this.sourceRoot = '',
    this.entryType = '',
    this.encryptionKey = '',
    this.raw = '',
  });
  factory WsCommand.fromJson(Map<String, dynamic> json) {
    return WsCommand(
      type: (json['type'] as String? ?? '').trim(),
      requestId: (json['requestId'] as String? ?? '').trim(),
      path: (json['path'] as String? ?? '').trim(),
      sourcePath: (json['source'] as String? ?? '').trim(),
      scopeId: (json['scopeId'] as String? ?? '').trim(),
      label: json['label'] as String? ?? '',
      storage: json['storage'] as String? ?? '',
      snapshot: json['snapshot'] as String? ?? '',
      relPath: json['relPath'] as String? ?? json['path'] as String? ?? '',
      outRoot: json['outRoot'] as String? ?? '',
      ticketId: json['ticketId'] as String? ?? '',
      downloadPathBase: json['downloadPathBase'] as String? ?? '',
      backupId: json['backupId'] as String? ?? '',
      backupRef: json['backupRef'] as String? ?? '',
      bundleId: json['bundleId'] as String? ?? '',
      archiveFile: json['archiveFile'] as String? ?? '',
      indexFile: json['indexFile'] as String? ?? '',
      packFile: json['packFile'] as String? ?? '',
      blobFiles: json['blobFiles'] as String? ?? '',
      sourceRoot: json['sourceRoot'] as String? ?? '',
      entryType: json['entryType'] as String? ?? '',
      encryptionKey: json['key'] as String? ?? '',
      raw: '',
    );
  }
  factory WsCommand.fromLegacy(String payload) {
    if (payload == 'ping') return WsCommand(type: 'ping', raw: payload);
    if (payload == 'state') return WsCommand(type: 'state', raw: payload);
    if (payload == 'snapshots') return WsCommand(type: 'snapshots', raw: payload);
    if (payload == 'fs.list') return WsCommand(type: 'fs.list', raw: payload);
    if (payload == 'fs.disks') return WsCommand(type: 'fs.disks', raw: payload);
    if (payload == 'backup') return WsCommand(type: 'backup', raw: payload);
    if (payload.startsWith('scan.scope:')) return WsCommand(type: 'scan.scope', scopeId: payload.substring(11), raw: payload);
    if (payload.startsWith('config.source:')) return WsCommand(type: 'config.source', path: payload.substring(14), raw: payload);
    if (payload.startsWith('config.archive:')) return WsCommand(type: 'config.archive', path: payload.substring(15), raw: payload);
    if (payload.startsWith('config.restoreRoot:')) return WsCommand(type: 'config.restoreRoot', path: payload.substring(19), raw: payload);
    if (payload.startsWith('backup:')) {
      final parts = payload.substring(7).split('|');
      String lbl = parts.isNotEmpty ? parts[0] : '';
      String stor = '';
      String src = '';
      String key = '';
      for (int i = 1; i < parts.length; i++) {
        if (parts[i].startsWith('storage=')) stor = parts[i].substring(8);
        if (parts[i].startsWith('source=')) src = parts[i].substring(7);
        if (parts[i].startsWith('key=')) key = parts[i].substring(4);
      }
      return WsCommand(type: 'backup', label: lbl, storage: stor, sourcePath: src, encryptionKey: key, raw: payload);
    }
    if (payload.startsWith('restore.file:')) {
      final parts = payload.substring(13).split('|');
      return WsCommand(
        type: 'restore.file',
        snapshot: parts.isNotEmpty ? parts[0] : '',
        relPath: parts.length > 1 ? parts[1] : '',
        outRoot: parts.length > 2 ? parts[2] : '',
        raw: payload,
      );
    }
    if (payload.startsWith('restore.snapshot:')) {
      final parts = payload.substring(17).split('|');
      return WsCommand(
        type: 'restore.snapshot',
        snapshot: parts.isNotEmpty ? parts[0] : '',
        outRoot: parts.length > 1 ? parts[1] : '',
        raw: payload,
      );
    }
    return WsCommand(type: 'unsupported', raw: payload);
  }
}
