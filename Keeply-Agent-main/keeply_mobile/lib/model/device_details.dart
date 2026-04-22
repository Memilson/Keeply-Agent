class DeviceDetails {
  final List<String> localIps;
  final String cpuModel;
  final String cpuArchitecture;
  final String kernelVersion;
  final int cpuCores;
  final int totalMemoryBytes;
  DeviceDetails({
    this.localIps = const [],
    this.cpuModel = '',
    this.cpuArchitecture = '',
    this.kernelVersion = '',
    this.cpuCores = 0,
    this.totalMemoryBytes = 0,
  });
  Map<String, dynamic> toJson() {
    return {
      'localIps': localIps,
      'hardware': {
        'cpuModel': cpuModel,
        'cpuArchitecture': cpuArchitecture,
        'kernelVersion': kernelVersion,
        'cpuCores': cpuCores,
        'totalMemoryBytes': totalMemoryBytes,
      },
    };
  }
}
