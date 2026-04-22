import 'dart:io';
import '../model/ws_command.dart';
import '../model/fs_entry.dart';
import 'ws_controller.dart';
class FsController {
  final WsController _ws;
  FsController(this._ws);
  void handleFsList(WsCommand cmd) {
    var targetPath = cmd.path.trim();
    if (targetPath.isEmpty) targetPath = _ws.appState.source;
    if (targetPath.isEmpty) targetPath = '/storage/emulated/0';
    try {
      final dir = Directory(targetPath);
      if (!dir.existsSync()) {
        _ws.sendJson({'type': 'fs.list', 'requestId': cmd.requestId, 'path': targetPath, 'error': 'caminho-nao-encontrado', 'items': []});
        return;
      }
      final entries = <FsEntry>[];
      final items = dir.listSync();
      var count = 0;
      for (final item in items) {
        if (count >= 120) break;
        if (item is Directory) {
          entries.add(FsEntry(name: item.uri.pathSegments.where((s) => s.isNotEmpty).last, path: item.path, kind: 'dir'));
          count++;
        }
      }
      entries.sort((a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()));
      final parentPath = dir.parent.path;
      _ws.sendJson({
        'type': 'fs.list',
        'requestId': cmd.requestId,
        'path': targetPath,
        'parentPath': parentPath,
        'truncated': count >= 120,
        'items': entries.map((e) => e.toJson()).toList(),
      });
    } catch (e) {
      _ws.sendJson({'type': 'fs.list', 'requestId': cmd.requestId, 'path': targetPath, 'error': e.toString(), 'items': []});
    }
  }
  void handleFsDisks(WsCommand cmd) {
    final entries = <FsEntry>[];
    final storagePath = '/storage/emulated/0';
    if (Directory(storagePath).existsSync()) {
      entries.add(FsEntry(name: 'Internal Storage', path: storagePath, kind: 'dir'));
    }
    try {
      final storageDir = Directory('/storage');
      if (storageDir.existsSync()) {
        for (final item in storageDir.listSync()) {
          if (item is Directory && item.path != '/storage/emulated' && item.path != '/storage/self') {
            entries.add(FsEntry(name: item.uri.pathSegments.where((s) => s.isNotEmpty).last, path: item.path, kind: 'dir'));
          }
        }
      }
    } catch (_) {}
    _ws.sendJson({
      'type': 'fs.disks',
      'requestId': cmd.requestId,
      'path': '',
      'parentPath': '',
      'truncated': false,
      'items': entries.map((e) => e.toJson()).toList(),
    });
  }
}
