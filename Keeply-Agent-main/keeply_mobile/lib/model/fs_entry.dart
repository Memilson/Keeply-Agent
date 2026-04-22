class FsEntry {
  final String name;
  final String path;
  final String kind;
  final int size;
  FsEntry({
    this.name = '',
    this.path = '',
    this.kind = 'file',
    this.size = 0,
  });
  bool get isDir => kind == 'dir';
  factory FsEntry.fromJson(Map<String, dynamic> json) {
    return FsEntry(
      name: json['name'] as String? ?? '',
      path: json['path'] as String? ?? '',
      kind: json['kind'] as String? ?? 'file',
      size: json['size'] as int? ?? 0,
    );
  }
  Map<String, dynamic> toJson() {
    return {
      'name': name,
      'path': path,
      'kind': kind,
      'size': size,
    };
  }
}
