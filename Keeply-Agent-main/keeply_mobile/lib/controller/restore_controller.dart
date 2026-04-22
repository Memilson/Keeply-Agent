import '../model/ws_command.dart';
import 'ws_controller.dart';
class RestoreController {
  final WsController _ws;
  RestoreController(this._ws);
  void handleRestoreFile(WsCommand cmd) {
    if (cmd.snapshot.isEmpty || cmd.relPath.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'restore.file requer snapshot e relPath.'});
      return;
    }
    final outRoot = cmd.outRoot.isNotEmpty ? cmd.outRoot : _ws.appState.restoreRoot;
    _ws.sendJson({'type': 'restore.file.started', 'requestId': cmd.requestId, 'snapshot': cmd.snapshot, 'path': cmd.relPath, 'outRoot': outRoot});
    _ws.addLog('[restore] arquivo ${cmd.relPath} do snapshot ${cmd.snapshot}');
    _ws.sendJson({'type': 'restore.file.finished', 'requestId': cmd.requestId, 'snapshot': cmd.snapshot, 'path': cmd.relPath, 'outRoot': outRoot});
  }
  void handleRestoreSnapshot(WsCommand cmd) {
    if (cmd.snapshot.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'restore.snapshot requer snapshot.'});
      return;
    }
    final outRoot = cmd.outRoot.isNotEmpty ? cmd.outRoot : _ws.appState.restoreRoot;
    _ws.sendJson({'type': 'restore.snapshot.started', 'requestId': cmd.requestId, 'snapshot': cmd.snapshot, 'outRoot': outRoot});
    _ws.addLog('[restore] snapshot ${cmd.snapshot}');
    _ws.sendJson({'type': 'restore.snapshot.finished', 'requestId': cmd.requestId, 'snapshot': cmd.snapshot, 'outRoot': outRoot});
  }
  void handleRestoreCloudSnapshot(WsCommand cmd) {
    if (cmd.snapshot.isEmpty || cmd.downloadPathBase.isEmpty || cmd.archiveFile.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'restore.cloud.snapshot requer snapshot, downloadPathBase e archiveFile.'});
      return;
    }
    final outRoot = cmd.outRoot.isNotEmpty ? cmd.outRoot : _ws.appState.restoreRoot;
    _ws.sendJson({'type': 'restore.cloud.started', 'requestId': cmd.requestId, 'backupId': cmd.backupId, 'bundleId': cmd.bundleId, 'snapshot': cmd.snapshot, 'outRoot': outRoot, 'filesTotal': 1});
    _ws.addLog('[restore.cloud] snapshot ${cmd.snapshot} de ${cmd.archiveFile}');
    _ws.sendJson({'type': 'restore.cloud.finished', 'requestId': cmd.requestId, 'backupId': cmd.backupId, 'backupRef': cmd.backupRef, 'bundleId': cmd.bundleId, 'snapshot': cmd.snapshot, 'outRoot': outRoot, 'path': cmd.relPath, 'entryType': cmd.entryType});
  }
}
