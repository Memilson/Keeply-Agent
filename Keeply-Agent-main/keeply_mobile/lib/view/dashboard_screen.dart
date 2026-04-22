import 'package:flutter/material.dart';
import '../controller/ws_controller.dart';
import 'widget/status_badge.dart';
import 'widget/schedule_card.dart';
class DashboardScreen extends StatefulWidget {
  final WsController ws;
  const DashboardScreen({super.key, required this.ws});
  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}
class _DashboardScreenState extends State<DashboardScreen> {
  @override
  void initState() {
    super.initState();
    widget.ws.addListener(_refresh);
  }
  @override
  void dispose() {
    widget.ws.removeListener(_refresh);
    super.dispose();
  }
  void _refresh() => setState(() {});
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0F172A),
      appBar: AppBar(
        title: const Text('Keeply Dashboard', style: TextStyle(fontWeight: FontWeight.w700, fontSize: 18, color: Colors.white)),
        backgroundColor: const Color(0xFF1E293B),
        elevation: 0,
        actions: [StatusBadge(connected: widget.ws.connected), const SizedBox(width: 12)],
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            _buildAgentCard(),
            const SizedBox(height: 12),
            _buildProgressCard(),
            const SizedBox(height: 12),
            _buildScopeCard(),
            const SizedBox(height: 12),
            ScheduleCard(ws: widget.ws),
            const SizedBox(height: 12),
            _buildSnapshotsCard(),
          ],
        ),
      ),
    );
  }
  Widget _buildAgentCard() {
    final config = widget.ws.config;
    return _card(
      icon: Icons.devices,
      iconColor: const Color(0xFF3B82F6),
      title: 'Agente',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _infoRow('Device ID', config.agentId.isNotEmpty ? config.agentId : '—'),
          _infoRow('Hostname', config.hostName),
          _infoRow('OS', config.osName),
          _infoRow('Status', widget.ws.connected ? 'Online' : 'Offline'),
        ],
      ),
    );
  }
  Widget _buildProgressCard() {
    final progress = widget.ws.currentProgress;
    if (progress == null) {
      return _card(
        icon: Icons.cloud_upload,
        iconColor: const Color(0xFF22C55E),
        title: 'Backup',
        child: const Text('Nenhum backup em andamento', style: TextStyle(color: Color(0xFF94A3B8), fontSize: 13)),
      );
    }
    return _card(
      icon: Icons.cloud_upload,
      iconColor: const Color(0xFFF59E0B),
      title: 'Backup em Progresso',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _infoRow('Fase', progress.phase),
          _infoRow('Arquivos', '${progress.filesCompleted}/${progress.filesQueued}'),
          _infoRow('Adicionados', '${progress.stats.added}'),
          _infoRow('Reutilizados', '${progress.stats.reused}'),
          _infoRow('Bytes lidos', '${(progress.stats.bytesRead / 1048576).toStringAsFixed(1)} MB'),
          const SizedBox(height: 8),
          ClipRRect(
            borderRadius: BorderRadius.circular(4),
            child: LinearProgressIndicator(
              value: progress.progressPercent,
              backgroundColor: const Color(0xFF334155),
              valueColor: const AlwaysStoppedAnimation<Color>(Color(0xFF3B82F6)),
              minHeight: 6,
            ),
          ),
        ],
      ),
    );
  }
  Widget _buildScopeCard() {
    final scope = widget.ws.appState.scanScope;
    return _card(
      icon: Icons.folder_open,
      iconColor: const Color(0xFFEC4899),
      title: 'Escopo de Scan',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _infoRow('ID', scope.id),
          _infoRow('Label', scope.label),
          _infoRow('Path', scope.resolvedPath.isNotEmpty ? scope.resolvedPath : '—'),
        ],
      ),
    );
  }
  Widget _buildSnapshotsCard() {
    final snaps = widget.ws.snapshots;
    return _card(
      icon: Icons.history,
      iconColor: const Color(0xFF8B5CF6),
      title: 'Execucoes Recentes (${snaps.length})',
      child: snaps.isEmpty
          ? const Text('Nenhum snapshot', style: TextStyle(color: Color(0xFF94A3B8), fontSize: 13))
          : Column(
              children: snaps.take(10).map((s) {
                return Padding(
                  padding: const EdgeInsets.only(bottom: 6),
                  child: Row(
                    children: [
                      Container(
                        width: 8, height: 8,
                        decoration: const BoxDecoration(color: Color(0xFF22C55E), shape: BoxShape.circle),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Text(
                          '${s.label.isNotEmpty ? s.label : "snap-${s.id}"} — ${s.createdAt}',
                          style: const TextStyle(color: Color(0xFFCBD5E1), fontSize: 12),
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                      Text('${s.fileCount} arq', style: const TextStyle(color: Color(0xFF64748B), fontSize: 11)),
                    ],
                  ),
                );
              }).toList(),
            ),
    );
  }
  Widget _card({required IconData icon, required Color iconColor, required String title, required Widget child}) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: const Color(0xFF1E293B),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF334155)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, color: iconColor, size: 18),
              const SizedBox(width: 8),
              Text(title, style: const TextStyle(color: Colors.white, fontWeight: FontWeight.w600, fontSize: 14)),
            ],
          ),
          const SizedBox(height: 12),
          child,
        ],
      ),
    );
  }
  Widget _infoRow(String label, String value) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 4),
      child: Row(
        children: [
          SizedBox(width: 100, child: Text(label, style: const TextStyle(color: Color(0xFF64748B), fontSize: 12))),
          Expanded(child: Text(value, style: const TextStyle(color: Color(0xFFE2E8F0), fontSize: 12), overflow: TextOverflow.ellipsis)),
        ],
      ),
    );
  }
}
