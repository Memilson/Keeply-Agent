package br.app.keeply;
import android.content.Context;
import android.os.Environment;
import android.os.storage.StorageManager;
import android.os.storage.StorageVolume;
import androidx.annotation.NonNull;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class FileSystemChannel implements MethodChannel.MethodCallHandler {
    private final Context context;
    public FileSystemChannel(Context context) {
        this.context = context;
    }
    @Override
    public void onMethodCall(@NonNull MethodCall call, @NonNull MethodChannel.Result result) {
        switch (call.method) {
            case "listDirectory":
                listDirectory(call.argument("path"), result);
                break;
            case "listStorageVolumes":
                listStorageVolumes(result);
                break;
            case "fileExists":
                result.success(new File(call.argument("path")).exists());
                break;
            case "fileSize":
                result.success(new File(call.argument("path")).length());
                break;
            case "lastModified":
                result.success(new File(call.argument("path")).lastModified());
                break;
            default:
                result.notImplemented();
        }
    }
    private void listDirectory(String path, MethodChannel.Result result) {
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) {
            result.success(new ArrayList<>());
            return;
        }
        File[] files = dir.listFiles();
        List<Map<String, Object>> entries = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                if (entries.size() >= 120) break;
                Map<String, Object> entry = new HashMap<>();
                entry.put("name", f.getName());
                entry.put("path", f.getAbsolutePath());
                entry.put("kind", f.isDirectory() ? "dir" : "file");
                entry.put("size", f.length());
                entries.add(entry);
            }
        }
        result.success(entries);
    }
    private void listStorageVolumes(MethodChannel.Result result) {
        List<Map<String, Object>> volumes = new ArrayList<>();
        Map<String, Object> internal = new HashMap<>();
        internal.put("name", "Internal Storage");
        internal.put("path", Environment.getExternalStorageDirectory().getAbsolutePath());
        internal.put("kind", "dir");
        internal.put("size", 0);
        volumes.add(internal);
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.N) {
            StorageManager sm = (StorageManager) context.getSystemService(Context.STORAGE_SERVICE);
            if (sm != null) {
                for (StorageVolume sv : sm.getStorageVolumes()) {
                    if (!sv.isPrimary()) {
                        Map<String, Object> vol = new HashMap<>();
                        vol.put("name", sv.getDescription(context));
                        vol.put("path", "/storage/" + sv.getUuid());
                        vol.put("kind", "dir");
                        vol.put("size", 0);
                        volumes.add(vol);
                    }
                }
            }
        }
        result.success(volumes);
    }
}
