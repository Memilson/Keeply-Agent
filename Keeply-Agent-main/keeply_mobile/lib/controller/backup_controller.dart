import '../model/ws_command.dart';
import '../model/backup_progress.dart';
import 'ws_controller.dart';
class BackupController {
  final WsController _ws;
  BackupController(this._ws);
  void handleBackup(WsCommand cmd) {
    final label = cmd.label.isNotEmpty ? cmd.label : 'mobile-backup';
    if (cmd.sourcePath.isNotEmpty) _ws.appState.source = cmd.sourcePath;
    _ws.addLog('[backup] iniciado | label=$label | source=${_ws.appState.source}');
    _ws.sendJson({
      'type': 'backup.started',
      'label': label,
      'source': _ws.appState.source,
      'scanScope': _ws.appState.scanScope.toJson(),
    });
    _simulateBackup(label);
  }
  Future<void> _simulateBackup(String label) async {
    final phases = ['discovery', 'backup', 'commit', 'done'];
    var progress = BackupProgress(phase: 'discovery');
    for (final phase in phases) {
      final idx = phases.indexOf(phase);
      progress = BackupProgress(
        phase: phase,
        filesQueued: 100,
        filesCompleted: phase == 'done' ? 100 : (idx * 33),
        discoveryComplete: phase != 'discovery',
        stats: BackupStats(
          scanned: phase == 'done' ? 100 : (idx * 33),
          added: phase == 'done' ? 15 : (idx * 5),
          reused: phase == 'done' ? 85 : (idx * 28),
          bytesRead: phase == 'done' ? 52428800 : (idx * 17476267),
        ),
      );
      _ws.updateProgress(progress);
      _ws.sendJson({
        'type': 'backup.progress',
        'phase': progress.phase,
        'discoveryComplete': progress.discoveryComplete,
        'currentFile': progress.currentFile,
        'filesQueued': progress.filesQueued,
        'filesCompleted': progress.filesCompleted,
        'filesScanned': progress.stats.scanned,
        'filesAdded': progress.stats.added,
        'filesUnchanged': progress.stats.reused,
        'chunksNew': progress.stats.uniqueChunksInserted,
        'bytesRead': progress.stats.bytesRead,
        'warnings': progress.stats.warnings,
      });
      await Future.delayed(const Duration(milliseconds: 800));
    }
    _ws.sendJson({
      'type': 'backup.finished',
      'label': label,
      'source': _ws.appState.source,
      'scanScope': _ws.appState.scanScope.toJson(),
      'filesScanned': progress.stats.scanned,
      'filesAdded': progress.stats.added,
      'filesUnchanged': progress.stats.reused,
      'chunksNew': progress.stats.uniqueChunksInserted,
      'chunksReused': 0,
      'bytesRead': progress.stats.bytesRead,
      'warnings': progress.stats.warnings,
    });
    _ws.addLog('[backup] concluido');
    _ws.updateProgress(null);
  }
}
