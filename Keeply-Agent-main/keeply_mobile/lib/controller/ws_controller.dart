import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter/foundation.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import '../model/ws_command.dart';
import '../model/ws_config.dart';
import '../model/app_state.dart';
import '../model/agent_identity.dart';
import '../model/backup_progress.dart';
import '../model/snapshot.dart';
import 'identity_controller.dart';
import 'config_controller.dart';
import 'backup_controller.dart';
import 'restore_controller.dart';
import 'fs_controller.dart';
class WsController extends ChangeNotifier {
  static const int _kProtocolVersion = 1;
  static const int _kReconnectMs = 5000;
  static const double _kJitterFactor = 0.1;
  static const int _kKeepAliveMs = 25000;
  static const int _kMaxMsgsPerMinute = 60;
  WebSocketChannel? _channel;
  WsConfig _config = WsConfig();
  AgentIdentity _identity = AgentIdentity();
  AppState _appState = AppState();
  bool _connected = false;
  bool _shouldReconnect = true;
  Timer? _keepAliveTimer;
  Timer? _reconnectTimer;
  final List<DateTime> _msgTimestamps = [];
  final List<String> _log = [];
  BackupProgress? _currentProgress;
  List<Snapshot> _snapshots = [];
  WsConfig get config => _config;
  AgentIdentity get identity => _identity;
  AppState get appState => _appState;
  bool get connected => _connected;
  List<String> get log => List.unmodifiable(_log);
  BackupProgress? get currentProgress => _currentProgress;
  List<Snapshot> get snapshots => _snapshots;
  late final ConfigController configController;
  late final BackupController backupController;
  late final RestoreController restoreController;
  late final FsController fsController;
  late final IdentityController identityController;
  WsController() {
    configController = ConfigController(this);
    backupController = BackupController(this);
    restoreController = RestoreController(this);
    fsController = FsController(this);
    identityController = IdentityController(this);
  }
  void addLog(String message) {
    final ts = DateTime.now().toIso8601String().substring(11, 19);
    _log.add('[$ts] $message');
    if (_log.length > 500) _log.removeAt(0);
    notifyListeners();
  }
  Future<void> connect(WsConfig config, AgentIdentity identity) async {
    _config = config;
    _identity = identity;
    _shouldReconnect = true;
    _appState.source = '/storage/emulated/0';
    _appState.archive = '${config.identityDir}/keeply.kipy';
    _appState.restoreRoot = '${config.identityDir}/restore';
    await _doConnect();
  }
  Future<void> _doConnect() async {
    try {
      _channel?.sink.close();
      final uri = Uri.parse('${_config.url}?deviceId=${Uri.encodeComponent(_config.agentId)}');
      addLog('Conectando a $uri');
      _channel = WebSocketChannel.connect(uri);
      await _channel!.ready;
      _connected = true;
      addLog('Conectado');
      notifyListeners();
      _sendHello();
      _startKeepAlive();
      _channel!.stream.listen(
        _onMessage,
        onError: (e) {
          addLog('Erro WS: $e');
          _onDisconnect();
        },
        onDone: _onDisconnect,
      );
    } catch (e) {
      addLog('Falha na conexao: $e');
      _onDisconnect();
    }
  }
  void _onDisconnect() {
    _connected = false;
    _keepAliveTimer?.cancel();
    _channel = null;
    notifyListeners();
    if (_shouldReconnect) _scheduleReconnect();
  }
  void _scheduleReconnect() {
    final jitterRange = (_kReconnectMs * _kJitterFactor).toInt();
    final jitter = jitterRange > 0 ? Random().nextInt(jitterRange * 2 + 1) - jitterRange : 0;
    final delayMs = max(100, _kReconnectMs + jitter);
    addLog('Reconectando em ${delayMs}ms');
    _reconnectTimer?.cancel();
    _reconnectTimer = Timer(Duration(milliseconds: delayMs), _doConnect);
  }
  void _startKeepAlive() {
    _keepAliveTimer?.cancel();
    _keepAliveTimer = Timer.periodic(const Duration(milliseconds: _kKeepAliveMs), (_) {
      if (_connected) sendText('keepalive');
    });
  }
  void _onMessage(dynamic data) {
    if (data is! String) return;
    final now = DateTime.now();
    _msgTimestamps.removeWhere((t) => now.difference(t).inSeconds > 60);
    if (_msgTimestamps.length >= _kMaxMsgsPerMinute) return;
    _msgTimestamps.add(now);
    addLog('← $data');
    _handleMessage(data);
  }
  void _handleMessage(String payload) {
    if (payload.isEmpty) return;
    try {
      final WsCommand cmd;
      if (payload.startsWith('{')) {
        final json = jsonDecode(payload) as Map<String, dynamic>;
        cmd = WsCommand.fromJson(json);
      } else {
        cmd = WsCommand.fromLegacy(payload);
      }
      _executeCommand(cmd);
    } catch (e) {
      sendJson({'type': 'error', 'message': e.toString()});
    }
  }
  void _executeCommand(WsCommand cmd) {
    switch (cmd.type) {
      case 'ping':
        sendJson({'type': 'pong'});
        break;
      case 'state':
        sendJson(_buildStateJson());
        break;
      case 'snapshots':
        sendJson(_buildSnapshotsJson());
        break;
      case 'fs.list':
        fsController.handleFsList(cmd);
        break;
      case 'fs.disks':
        fsController.handleFsDisks(cmd);
        break;
      case 'scan.scope':
        configController.handleScanScope(cmd);
        break;
      case 'config.source':
        configController.handleConfigSource(cmd);
        break;
      case 'config.archive':
        configController.handleConfigArchive(cmd);
        break;
      case 'config.restoreRoot':
        configController.handleConfigRestoreRoot(cmd);
        break;
      case 'backup':
        backupController.handleBackup(cmd);
        break;
      case 'restore.file':
        restoreController.handleRestoreFile(cmd);
        break;
      case 'restore.snapshot':
        restoreController.handleRestoreSnapshot(cmd);
        break;
      case 'restore.cloud.snapshot':
        restoreController.handleRestoreCloudSnapshot(cmd);
        break;
      default:
        sendJson({'type': 'error', 'message': 'Comando nao suportado: ${cmd.type}'});
    }
  }
  void _sendHello() {
    final hello = <String, dynamic>{
      'type': 'agent.hello',
      'protocolVersion': _kProtocolVersion,
      'deviceId': _config.agentId,
      'agentId': _config.agentId,
      'osName': _config.osName,
      'hostName': _config.hostName,
      'fingerprintSha256': _identity.fingerprintSha256,
      'connectedAt': DateTime.now().toIso8601String().replaceFirst('T', ' ').substring(0, 19),
      'source': _appState.source,
      'archive': _appState.archive,
      'restoreRoot': _appState.restoreRoot,
      'deviceDetails': {
        'localIps': _config.localIps,
        'hardware': {
          'cpuModel': _config.cpuModel,
          'cpuArchitecture': _config.cpuArchitecture,
          'kernelVersion': _config.kernelVersion,
          'cpuCores': _config.cpuCores,
          'totalMemoryBytes': _config.totalMemoryBytes,
        },
      },
      'scanScope': _appState.scanScope.toJson(),
    };
    sendJson(hello);
  }
  Map<String, dynamic> _buildStateJson() {
    return {
      'type': 'state',
      'deviceId': _config.agentId,
      'agentId': _config.agentId,
      'source': _appState.source,
      'archive': _appState.archive,
      'restoreRoot': _appState.restoreRoot,
      'deviceDetails': {
        'localIps': _config.localIps,
        'hardware': {
          'cpuModel': _config.cpuModel,
          'cpuArchitecture': _config.cpuArchitecture,
          'kernelVersion': _config.kernelVersion,
          'cpuCores': _config.cpuCores,
          'totalMemoryBytes': _config.totalMemoryBytes,
        },
      },
      'scanScope': _appState.scanScope.toJson(),
      'archiveSplitEnabled': _appState.archiveSplitEnabled,
      'archiveSplitMaxBytes': _appState.archiveSplitMaxBytes,
    };
  }
  Map<String, dynamic> _buildSnapshotsJson() {
    return {
      'type': 'snapshots',
      'deviceId': _config.agentId,
      'agentId': _config.agentId,
      'items': _snapshots.map((s) => s.toJson()).toList(),
    };
  }
  void updateProgress(BackupProgress progress) {
    _currentProgress = progress;
    notifyListeners();
  }
  void updateSnapshots(List<Snapshot> list) {
    _snapshots = list;
    notifyListeners();
  }
  void sendJson(Map<String, dynamic> payload) {
    if (payload.containsKey('type')) {
      payload['v'] = _kProtocolVersion;
    }
    final text = jsonEncode(payload);
    sendText(text);
  }
  void sendText(String payload) {
    if (!_connected || _channel == null) return;
    try {
      _channel!.sink.add(payload);
      addLog('→ $payload');
    } catch (e) {
      addLog('Erro ao enviar: $e');
    }
  }
  void sendCommand(String type, {Map<String, dynamic>? extra}) {
    final msg = <String, dynamic>{'type': type};
    if (extra != null) msg.addAll(extra);
    sendJson(msg);
  }
  void disconnect() {
    _shouldReconnect = false;
    _keepAliveTimer?.cancel();
    _reconnectTimer?.cancel();
    _channel?.sink.close();
    _connected = false;
    notifyListeners();
  }
  @override
  void dispose() {
    disconnect();
    super.dispose();
  }
}
