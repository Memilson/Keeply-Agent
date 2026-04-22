import 'package:flutter/material.dart';
class StatusBadge extends StatelessWidget {
  final bool connected;
  const StatusBadge({super.key, required this.connected});
  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
      decoration: BoxDecoration(
        color: connected ? const Color(0xFF22C55E).withOpacity(0.15) : const Color(0xFFEF4444).withOpacity(0.15),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: connected ? const Color(0xFF22C55E).withOpacity(0.4) : const Color(0xFFEF4444).withOpacity(0.4)),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 6, height: 6,
            decoration: BoxDecoration(
              color: connected ? const Color(0xFF22C55E) : const Color(0xFFEF4444),
              shape: BoxShape.circle,
            ),
          ),
          const SizedBox(width: 6),
          Text(
            connected ? 'Online' : 'Offline',
            style: TextStyle(
              color: connected ? const Color(0xFF22C55E) : const Color(0xFFEF4444),
              fontSize: 11,
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}
