class ScanScope {
  final String id;
  final String label;
  final String requestedPath;
  final String resolvedPath;
  ScanScope({
    this.id = 'home',
    this.label = 'Home',
    this.requestedPath = '',
    this.resolvedPath = '',
  });
  factory ScanScope.fromJson(Map<String, dynamic> json) {
    return ScanScope(
      id: json['id'] as String? ?? 'home',
      label: json['label'] as String? ?? 'Home',
      requestedPath: json['requestedPath'] as String? ?? '',
      resolvedPath: json['resolvedPath'] as String? ?? '',
    );
  }
  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'label': label,
      'requestedPath': requestedPath,
      'resolvedPath': resolvedPath,
    };
  }
  ScanScope copyWith({String? id, String? label, String? requestedPath, String? resolvedPath}) {
    return ScanScope(
      id: id ?? this.id,
      label: label ?? this.label,
      requestedPath: requestedPath ?? this.requestedPath,
      resolvedPath: resolvedPath ?? this.resolvedPath,
    );
  }
}
