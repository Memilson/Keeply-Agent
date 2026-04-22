class ChangeEntry {
  final String path;
  final String status;
  final int oldSize;
  final int newSize;
  final int oldMtime;
  final int newMtime;
  ChangeEntry({
    this.path = '',
    this.status = '',
    this.oldSize = 0,
    this.newSize = 0,
    this.oldMtime = 0,
    this.newMtime = 0,
  });
  factory ChangeEntry.fromJson(Map<String, dynamic> json) {
    return ChangeEntry(
      path: json['path'] as String? ?? '',
      status: json['status'] as String? ?? '',
      oldSize: json['oldSize'] as int? ?? 0,
      newSize: json['newSize'] as int? ?? 0,
      oldMtime: json['oldMtime'] as int? ?? 0,
      newMtime: json['newMtime'] as int? ?? 0,
    );
  }
}
