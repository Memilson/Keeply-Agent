import 'package:flutter/material.dart';
import '../controller/ws_controller.dart';
import 'widget/log_panel.dart';
import 'widget/command_tile.dart';
import 'widget/status_badge.dart';
class TestScreen extends StatefulWidget {
  final WsController ws;
  const TestScreen({super.key, required this.ws});
  @override
  State<TestScreen> createState() => _TestScreenState();
}
class _TestScreenState extends State<TestScreen> {
  final _labelCtrl = TextEditingController(text: 'test-backup');
  final _pathCtrl = TextEditingController(text: '/storage/emulated/0');
  final _snapshotCtrl = TextEditingController(text: '1');
  final _scopeCtrl = TextEditingController(text: 'documents');
  @override
  void initState() {
    super.initState();
    widget.ws.addListener(_refresh);
  }
  @override
  void dispose() {
    widget.ws.removeListener(_refresh);
    _labelCtrl.dispose();
    _pathCtrl.dispose();
    _snapshotCtrl.dispose();
    _scopeCtrl.dispose();
    super.dispose();
  }
  void _refresh() => setState(() {});
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0F172A),
      appBar: AppBar(
        title: const Text('Keeply Test', style: TextStyle(fontWeight: FontWeight.w700, fontSize: 18, color: Colors.white)),
        backgroundColor: const Color(0xFF1E293B),
        elevation: 0,
        actions: [StatusBadge(connected: widget.ws.connected), const SizedBox(width: 12)],
      ),
      body: Column(
        children: [
          _buildInputBar(),
          _buildCommandGrid(),
          const Divider(color: Color(0xFF334155), height: 1),
          Expanded(child: LogPanel(logs: widget.ws.log)),
        ],
      ),
    );
  }
  Widget _buildInputBar() {
    return Container(
      color: const Color(0xFF1E293B),
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      child: Row(
        children: [
          _miniField(_labelCtrl, 'Label', 80),
          const SizedBox(width: 6),
          _miniField(_pathCtrl, 'Path', 120),
          const SizedBox(width: 6),
          _miniField(_snapshotCtrl, 'Snap', 50),
          const SizedBox(width: 6),
          _miniField(_scopeCtrl, 'Scope', 80),
        ],
      ),
    );
  }
  Widget _miniField(TextEditingController ctrl, String hint, double w) {
    return SizedBox(
      width: w,
      height: 32,
      child: TextField(
        controller: ctrl,
        style: const TextStyle(color: Colors.white, fontSize: 11),
        decoration: InputDecoration(
          hintText: hint,
          hintStyle: const TextStyle(color: Color(0xFF64748B), fontSize: 11),
          filled: true,
          fillColor: const Color(0xFF0F172A),
          contentPadding: const EdgeInsets.symmetric(horizontal: 8, vertical: 0),
          border: OutlineInputBorder(borderRadius: BorderRadius.circular(6), borderSide: const BorderSide(color: Color(0xFF334155))),
          enabledBorder: OutlineInputBorder(borderRadius: BorderRadius.circular(6), borderSide: const BorderSide(color: Color(0xFF334155))),
          focusedBorder: OutlineInputBorder(borderRadius: BorderRadius.circular(6), borderSide: const BorderSide(color: Color(0xFF3B82F6))),
        ),
      ),
    );
  }
  Widget _buildCommandGrid() {
    return Container(
      color: const Color(0xFF1E293B),
      padding: const EdgeInsets.fromLTRB(12, 0, 12, 8),
      child: Wrap(
        spacing: 6,
        runSpacing: 6,
        children: [
          CommandTile(label: 'ping', color: const Color(0xFF22C55E), onTap: () => widget.ws.sendCommand('ping')),
          CommandTile(label: 'state', color: const Color(0xFF3B82F6), onTap: () => widget.ws.sendCommand('state')),
          CommandTile(label: 'snapshots', color: const Color(0xFF8B5CF6), onTap: () => widget.ws.sendCommand('snapshots')),
          CommandTile(label: 'backup', color: const Color(0xFFF59E0B), onTap: () => widget.ws.sendCommand('backup', extra: {'label': _labelCtrl.text})),
          CommandTile(label: 'fs.list', color: const Color(0xFF06B6D4), onTap: () => widget.ws.sendCommand('fs.list', extra: {'path': _pathCtrl.text})),
          CommandTile(label: 'fs.disks', color: const Color(0xFF06B6D4), onTap: () => widget.ws.sendCommand('fs.disks')),
          CommandTile(label: 'scan.scope', color: const Color(0xFFEC4899), onTap: () => widget.ws.sendCommand('scan.scope', extra: {'scopeId': _scopeCtrl.text})),
          CommandTile(label: 'restore.file', color: const Color(0xFFEF4444), onTap: () => widget.ws.sendCommand('restore.file', extra: {'snapshot': _snapshotCtrl.text, 'relPath': _pathCtrl.text})),
          CommandTile(label: 'restore.snap', color: const Color(0xFFEF4444), onTap: () => widget.ws.sendCommand('restore.snapshot', extra: {'snapshot': _snapshotCtrl.text})),
        ],
      ),
    );
  }
}
