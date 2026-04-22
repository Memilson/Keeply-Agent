class BackupStats {
  int scanned;
  int added;
  int reused;
  int chunks;
  int uniqueChunksInserted;
  int bytesRead;
  int warnings;
  BackupStats({
    this.scanned = 0,
    this.added = 0,
    this.reused = 0,
    this.chunks = 0,
    this.uniqueChunksInserted = 0,
    this.bytesRead = 0,
    this.warnings = 0,
  });
  factory BackupStats.fromJson(Map<String, dynamic> json) {
    return BackupStats(
      scanned: json['filesScanned'] as int? ?? 0,
      added: json['filesAdded'] as int? ?? 0,
      reused: json['filesUnchanged'] as int? ?? 0,
      chunks: json['chunks'] as int? ?? 0,
      uniqueChunksInserted: json['chunksNew'] as int? ?? 0,
      bytesRead: json['bytesRead'] as int? ?? 0,
      warnings: json['warnings'] as int? ?? 0,
    );
  }
}
class BackupProgress {
  BackupStats stats;
  int filesQueued;
  int filesCompleted;
  bool discoveryComplete;
  String phase;
  String currentFile;
  BackupProgress({
    BackupStats? stats,
    this.filesQueued = 0,
    this.filesCompleted = 0,
    this.discoveryComplete = false,
    this.phase = 'idle',
    this.currentFile = '',
  }) : stats = stats ?? BackupStats();
  factory BackupProgress.fromJson(Map<String, dynamic> json) {
    return BackupProgress(
      stats: BackupStats.fromJson(json),
      filesQueued: json['filesQueued'] as int? ?? 0,
      filesCompleted: json['filesCompleted'] as int? ?? 0,
      discoveryComplete: json['discoveryComplete'] as bool? ?? false,
      phase: json['phase'] as String? ?? 'idle',
      currentFile: json['currentFile'] as String? ?? '',
    );
  }
  double get progressPercent {
    if (filesQueued <= 0) return 0.0;
    return (filesCompleted / filesQueued).clamp(0.0, 1.0);
  }
}
