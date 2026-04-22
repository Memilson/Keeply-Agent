import 'package:flutter/material.dart';
class LogPanel extends StatefulWidget {
  final List<String> logs;
  const LogPanel({super.key, required this.logs});
  @override
  State<LogPanel> createState() => _LogPanelState();
}
class _LogPanelState extends State<LogPanel> {
  final _scrollCtrl = ScrollController();
  @override
  void didUpdateWidget(covariant LogPanel oldWidget) {
    super.didUpdateWidget(oldWidget);
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollCtrl.hasClients) {
        _scrollCtrl.animateTo(_scrollCtrl.position.maxScrollExtent, duration: const Duration(milliseconds: 150), curve: Curves.easeOut);
      }
    });
  }
  @override
  void dispose() {
    _scrollCtrl.dispose();
    super.dispose();
  }
  @override
  Widget build(BuildContext context) {
    return Container(
      color: const Color(0xFF0F172A),
      child: ListView.builder(
        controller: _scrollCtrl,
        padding: const EdgeInsets.all(8),
        itemCount: widget.logs.length,
        itemBuilder: (ctx, i) {
          final line = widget.logs[i];
          final isOutgoing = line.contains('→');
          final isError = line.toLowerCase().contains('erro') || line.toLowerCase().contains('falha');
          final color = isError ? const Color(0xFFEF4444) : isOutgoing ? const Color(0xFF22C55E) : const Color(0xFF94A3B8);
          return Padding(
            padding: const EdgeInsets.only(bottom: 2),
            child: Text(line, style: TextStyle(color: color, fontSize: 10, fontFamily: 'monospace')),
          );
        },
      ),
    );
  }
}
