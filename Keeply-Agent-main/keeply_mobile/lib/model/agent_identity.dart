class AgentIdentity {
  String deviceId;
  String userId;
  String fingerprintSha256;
  String pairingCode;
  String certPemPath;
  String keyPemPath;
  AgentIdentity({
    this.deviceId = '',
    this.userId = '',
    this.fingerprintSha256 = '',
    this.pairingCode = '',
    this.certPemPath = '',
    this.keyPemPath = '',
  });
  bool get isPaired => userId.isNotEmpty;
  bool get hasCertificate => certPemPath.isNotEmpty && keyPemPath.isNotEmpty;
  Map<String, String> toMeta() {
    return {
      'device_id': deviceId,
      'user_id': userId,
      'fingerprint_sha256': fingerprintSha256,
      'pairing_code': pairingCode,
      'cert_pem': certPemPath,
      'key_pem': keyPemPath,
    };
  }
  factory AgentIdentity.fromMeta(Map<String, String> meta) {
    return AgentIdentity(
      deviceId: meta['device_id'] ?? '',
      userId: meta['user_id'] ?? '',
      fingerprintSha256: meta['fingerprint_sha256'] ?? '',
      pairingCode: meta['pairing_code'] ?? '',
      certPemPath: meta['cert_pem'] ?? '',
      keyPemPath: meta['key_pem'] ?? '',
    );
  }
}
