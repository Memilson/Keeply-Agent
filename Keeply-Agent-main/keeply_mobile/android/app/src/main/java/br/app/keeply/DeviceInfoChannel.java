package br.app.keeply;
import android.app.ActivityManager;
import android.content.Context;
import android.os.Build;
import androidx.annotation.NonNull;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class DeviceInfoChannel implements MethodChannel.MethodCallHandler {
    private final Context context;
    public DeviceInfoChannel(Context context) {
        this.context = context;
    }
    @Override
    public void onMethodCall(@NonNull MethodCall call, @NonNull MethodChannel.Result result) {
        switch (call.method) {
            case "getDeviceInfo":
                result.success(getDeviceInfo());
                break;
            default:
                result.notImplemented();
        }
    }
    private Map<String, Object> getDeviceInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("hostname", Build.MODEL);
        info.put("osName", "android");
        info.put("cpuModel", Build.HARDWARE);
        info.put("cpuArchitecture", Build.SUPPORTED_ABIS.length > 0 ? Build.SUPPORTED_ABIS[0] : "unknown");
        info.put("kernelVersion", System.getProperty("os.version"));
        info.put("cpuCores", Runtime.getRuntime().availableProcessors());
        ActivityManager am = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        ActivityManager.MemoryInfo mi = new ActivityManager.MemoryInfo();
        if (am != null) {
            am.getMemoryInfo(mi);
            info.put("totalMemoryBytes", mi.totalMem);
        } else {
            info.put("totalMemoryBytes", 0L);
        }
        info.put("localIps", getLocalIps());
        return info;
    }
    private List<String> getLocalIps() {
        List<String> ips = new ArrayList<>();
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (InetAddress addr : Collections.list(ni.getInetAddresses())) {
                    if (addr.isLoopbackAddress()) continue;
                    String ip = addr.getHostAddress();
                    if (ip != null && !ip.contains("%")) ips.add(ip);
                }
            }
        } catch (Exception ignored) {}
        return ips;
    }
}
