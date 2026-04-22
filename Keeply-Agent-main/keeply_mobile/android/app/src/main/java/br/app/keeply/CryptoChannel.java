package br.app.keeply;
import androidx.annotation.NonNull;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import java.io.FileOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.x500.X500Principal;
public class CryptoChannel implements MethodChannel.MethodCallHandler {
    @Override
    public void onMethodCall(@NonNull MethodCall call, @NonNull MethodChannel.Result result) {
        switch (call.method) {
            case "generateIdentity":
                generateIdentity(call.argument("commonName"), call.argument("certPath"), call.argument("keyPath"), result);
                break;
            case "computeFingerprint":
                computeFingerprint(call.argument("certPath"), result);
                break;
            default:
                result.notImplemented();
        }
    }
    private void generateIdentity(String cn, String certPath, String keyPath, MethodChannel.Result result) {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair kp = kpg.generateKeyPair();
            byte[] keyBytes = kp.getPrivate().getEncoded();
            try (FileOutputStream fos = new FileOutputStream(keyPath)) {
                fos.write(keyBytes);
            }
            byte[] pubBytes = kp.getPublic().getEncoded();
            try (FileOutputStream fos = new FileOutputStream(certPath)) {
                fos.write(pubBytes);
            }
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(pubBytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            Map<String, String> res = new HashMap<>();
            res.put("fingerprint", sb.toString());
            result.success(res);
        } catch (Exception e) {
            result.error("CRYPTO_ERROR", e.getMessage(), null);
        }
    }
    private void computeFingerprint(String certPath, MethodChannel.Result result) {
        try {
            java.io.FileInputStream fis = new java.io.FileInputStream(certPath);
            byte[] data = fis.readAllBytes();
            fis.close();
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            result.success(sb.toString());
        } catch (Exception e) {
            result.error("CRYPTO_ERROR", e.getMessage(), null);
        }
    }
}
