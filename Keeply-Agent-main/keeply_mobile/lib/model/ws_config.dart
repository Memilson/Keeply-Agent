class WsConfig {
  String url;
  String agentId;
  String deviceName;
  String hostName;
  String osName;
  List<String> localIps;
  String cpuModel;
  String cpuArchitecture;
  String kernelVersion;
  int totalMemoryBytes;
  int cpuCores;
  String pairingCode;
  String identityDir;
  bool allowInsecureTls;
  int pairingPollIntervalMs;
  WsConfig({
    this.url = 'wss://backend.keeply.app.br/ws/agent',
    this.agentId = '',
    this.deviceName = 'keeply-agent',
    this.hostName = 'keeply-host',
    this.osName = 'android',
    this.localIps = const [],
    this.cpuModel = '',
    this.cpuArchitecture = '',
    this.kernelVersion = '',
    this.totalMemoryBytes = 0,
    this.cpuCores = 0,
    this.pairingCode = '',
    this.identityDir = '',
    this.allowInsecureTls = false,
    this.pairingPollIntervalMs = 3000,
  });
}
