import 'scan_scope.dart';
class AppState {
  String source;
  String archive;
  String restoreRoot;
  ScanScope scanScope;
  bool archiveSplitEnabled;
  int archiveSplitMaxBytes;
  AppState({
    this.source = '',
    this.archive = '',
    this.restoreRoot = '',
    ScanScope? scanScope,
    this.archiveSplitEnabled = false,
    this.archiveSplitMaxBytes = 0,
  }) : scanScope = scanScope ?? ScanScope();
  factory AppState.fromJson(Map<String, dynamic> json) {
    return AppState(
      source: json['source'] as String? ?? '',
      archive: json['archive'] as String? ?? '',
      restoreRoot: json['restoreRoot'] as String? ?? '',
      scanScope: json['scanScope'] != null ? ScanScope.fromJson(json['scanScope'] as Map<String, dynamic>) : ScanScope(),
      archiveSplitEnabled: json['archiveSplitEnabled'] as bool? ?? false,
      archiveSplitMaxBytes: json['archiveSplitMaxBytes'] as int? ?? 0,
    );
  }
  Map<String, dynamic> toJson() {
    return {
      'source': source,
      'archive': archive,
      'restoreRoot': restoreRoot,
      'scanScope': scanScope.toJson(),
      'archiveSplitEnabled': archiveSplitEnabled,
      'archiveSplitMaxBytes': archiveSplitMaxBytes,
    };
  }
}
