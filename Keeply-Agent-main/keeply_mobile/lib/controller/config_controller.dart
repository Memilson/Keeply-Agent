import '../model/ws_command.dart';
import '../model/scan_scope.dart';
import 'ws_controller.dart';
class ConfigController {
  final WsController _ws;
  ConfigController(this._ws);
  void handleScanScope(WsCommand cmd) {
    if (cmd.scopeId.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'scan.scope requer scopeId.'});
      return;
    }
    final scope = _resolveScanScope(cmd.scopeId);
    _ws.appState.scanScope = scope;
    _ws.appState.source = scope.resolvedPath;
    _ws.sendJson({
      'type': 'scan.scope.updated',
      'agentId': _ws.config.agentId,
      'source': _ws.appState.source,
      'scanScope': scope.toJson(),
    });
    _ws.notifyListeners();
  }
  void handleConfigSource(WsCommand cmd) {
    if (cmd.path.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'source nao pode ser vazio.'});
      return;
    }
    _ws.appState.source = cmd.path;
    _ws.appState.scanScope = ScanScope(id: 'custom', label: 'Custom', requestedPath: cmd.path, resolvedPath: cmd.path);
    _ws.sendJson({
      'type': 'config.updated',
      'field': 'source',
      'agentId': _ws.config.agentId,
      'source': _ws.appState.source,
      'scanScope': _ws.appState.scanScope.toJson(),
    });
    _ws.notifyListeners();
  }
  void handleConfigArchive(WsCommand cmd) {
    if (cmd.path.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'archive nao pode ser vazio.'});
      return;
    }
    _ws.appState.archive = cmd.path;
    _ws.sendJson({'type': 'config.updated', 'field': 'archive'});
    _ws.notifyListeners();
  }
  void handleConfigRestoreRoot(WsCommand cmd) {
    if (cmd.path.isEmpty) {
      _ws.sendJson({'type': 'error', 'message': 'restoreRoot nao pode ser vazio.'});
      return;
    }
    _ws.appState.restoreRoot = cmd.path;
    _ws.sendJson({'type': 'config.updated', 'field': 'restoreRoot'});
    _ws.notifyListeners();
  }
  ScanScope _resolveScanScope(String scopeId) {
    final id = scopeId.trim().toLowerCase();
    const scopeMap = {
      'home': {'label': 'Home', 'path': '/storage/emulated/0'},
      'documents': {'label': 'Documents', 'path': '/storage/emulated/0/Documents'},
      'desktop': {'label': 'Desktop', 'path': '/storage/emulated/0'},
      'downloads': {'label': 'Downloads', 'path': '/storage/emulated/0/Download'},
      'pictures': {'label': 'Pictures', 'path': '/storage/emulated/0/Pictures'},
      'music': {'label': 'Music', 'path': '/storage/emulated/0/Music'},
      'videos': {'label': 'Videos', 'path': '/storage/emulated/0/Movies'},
    };
    final entry = scopeMap[id];
    if (entry == null) {
      return ScanScope(id: 'home', label: 'Home', resolvedPath: '/storage/emulated/0');
    }
    return ScanScope(id: id, label: entry['label']!, resolvedPath: entry['path']!);
  }
}
