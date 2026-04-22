import 'dart:convert';
import 'dart:math';
import 'package:http/http.dart' as http;
import 'package:shared_preferences/shared_preferences.dart';
import '../model/agent_identity.dart';
import '../model/ws_config.dart';
import 'ws_controller.dart';
class IdentityController {
  final WsController _ws;
  IdentityController(this._ws);
  Future<AgentIdentity> ensureRegistered(WsConfig config) async {
    AgentIdentity identity = await _loadPersistedIdentity();
    if (identity.deviceId.isEmpty) {
      identity.deviceId = _generateDeviceId();
    }
    if (identity.fingerprintSha256.isEmpty) {
      identity.fingerprintSha256 = _generateFakeFingerprint();
    }
    if (identity.isPaired) {
      identity.pairingCode = '';
      await _saveIdentity(identity);
      return identity;
    }
    while (true) {
      if (identity.pairingCode.isEmpty) {
        identity.pairingCode = config.pairingCode.isNotEmpty ? config.pairingCode : _randomDigits(8);
      }
      try {
        final started = await _startPairing(config, identity);
        if (started['status'] == 'code_conflict') {
          identity.pairingCode = '';
          continue;
        }
        if ((started['code'] as String?)?.isNotEmpty == true) {
          identity.pairingCode = started['code'] as String;
        }
        if (started['status'] == 'active') {
          identity.deviceId = started['deviceId'] as String? ?? identity.deviceId;
          identity.userId = started['userId'] as String? ?? '';
          identity.pairingCode = '';
          break;
        }
        await _saveIdentity(identity);
        _ws.addLog('Codigo de pareamento: ${identity.pairingCode}');
        final confirmed = await _awaitPairingConfirmation(config, identity);
        if (confirmed) break;
      } catch (e) {
        _ws.addLog('Erro no pairing: $e');
        identity.pairingCode = '';
        await Future.delayed(const Duration(seconds: 5));
      }
    }
    await _saveIdentity(identity);
    return identity;
  }
  String _generateDeviceId() {
    final rng = Random.secure();
    final bytes = List.generate(16, (_) => rng.nextInt(256));
    return 'dev_${bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join()}';
  }
  String _generateFakeFingerprint() {
    final rng = Random.secure();
    final bytes = List.generate(32, (_) => rng.nextInt(256));
    return bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  }
  String _randomDigits(int count) {
    final rng = Random.secure();
    return List.generate(count, (_) => rng.nextInt(10).toString()).join();
  }
  String _httpUrlFromWsUrl(String wsUrl, String path) {
    String base = wsUrl;
    if (base.startsWith('wss://')) base = 'https://${base.substring(6)}';
    else if (base.startsWith('ws://')) base = 'http://${base.substring(5)}';
    final uri = Uri.parse(base);
    return '${uri.scheme}://${uri.host}:${uri.port}$path';
  }
  Future<Map<String, dynamic>> _startPairing(WsConfig config, AgentIdentity identity) async {
    final url = _httpUrlFromWsUrl(config.url, '/api/devices/pairing/start');
    final body = jsonEncode({
      'code': identity.pairingCode,
      'deviceName': config.deviceName,
      'hostName': config.hostName,
      'os': config.osName,
      'deviceId': identity.deviceId,
      'userId': identity.userId,
      'certFingerprintSha256': identity.fingerprintSha256,
    });
    final resp = await http.post(Uri.parse(url), headers: {'Content-Type': 'application/json'}, body: body);
    if (resp.statusCode == 409) return {'status': 'code_conflict'};
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      throw Exception('Pairing start falhou: HTTP ${resp.statusCode}');
    }
    return jsonDecode(resp.body) as Map<String, dynamic>;
  }
  Future<bool> _awaitPairingConfirmation(WsConfig config, AgentIdentity identity) async {
    final interval = Duration(milliseconds: max(1000, config.pairingPollIntervalMs));
    while (true) {
      await Future.delayed(interval);
      final url = _httpUrlFromWsUrl(config.url, '/api/devices/pairing/status');
      final body = jsonEncode({
        'code': identity.pairingCode,
        'deviceId': identity.deviceId,
        'userId': identity.userId,
        'certFingerprintSha256': identity.fingerprintSha256,
      });
      final resp = await http.post(Uri.parse(url), headers: {'Content-Type': 'application/json'}, body: body);
      if (resp.statusCode < 200 || resp.statusCode >= 300) continue;
      final data = jsonDecode(resp.body) as Map<String, dynamic>;
      final status = data['status'] as String? ?? '';
      if (status == 'active') {
        identity.deviceId = data['deviceId'] as String? ?? identity.deviceId;
        identity.userId = data['userId'] as String? ?? '';
        identity.pairingCode = '';
        return true;
      }
      if (status == 'expired' || status == 'missing') {
        identity.pairingCode = '';
        return false;
      }
    }
  }
  Future<void> _saveIdentity(AgentIdentity identity) async {
    final prefs = await SharedPreferences.getInstance();
    final meta = identity.toMeta();
    for (final entry in meta.entries) {
      await prefs.setString('keeply_${entry.key}', entry.value);
    }
  }
  Future<AgentIdentity> _loadPersistedIdentity() async {
    final prefs = await SharedPreferences.getInstance();
    final keys = ['device_id', 'user_id', 'fingerprint_sha256', 'pairing_code', 'cert_pem', 'key_pem'];
    final meta = <String, String>{};
    for (final key in keys) {
      meta[key] = prefs.getString('keeply_$key') ?? '';
    }
    return AgentIdentity.fromMeta(meta);
  }
}
