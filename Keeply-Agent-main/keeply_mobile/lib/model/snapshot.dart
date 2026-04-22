class Snapshot {
  final int id;
  final String createdAt;
  final String sourceRoot;
  final String label;
  final String backupType;
  final int fileCount;
  Snapshot({
    this.id = 0,
    this.createdAt = '',
    this.sourceRoot = '',
    this.label = '',
    this.backupType = '',
    this.fileCount = 0,
  });
  factory Snapshot.fromJson(Map<String, dynamic> json) {
    return Snapshot(
      id: json['id'] as int? ?? 0,
      createdAt: json['createdAt'] as String? ?? '',
      sourceRoot: json['sourceRoot'] as String? ?? '',
      label: json['label'] as String? ?? '',
      backupType: json['backupType'] as String? ?? '',
      fileCount: json['fileCount'] as int? ?? 0,
    );
  }
  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'createdAt': createdAt,
      'sourceRoot': sourceRoot,
      'label': label,
      'backupType': backupType,
      'fileCount': fileCount,
    };
  }
}
