import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'controller/ws_controller.dart';
import 'model/ws_config.dart';
import 'model/agent_identity.dart';
import 'view/test_screen.dart';
import 'view/dashboard_screen.dart';
void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const KeeplyApp());
}
class KeeplyApp extends StatelessWidget {
  const KeeplyApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Keeply Agent',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF0F172A),
        colorScheme: const ColorScheme.dark(
          primary: Color(0xFF3B82F6),
          secondary: Color(0xFF06B6D4),
          surface: Color(0xFF1E293B),
        ),
        fontFamily: 'Roboto',
      ),
      home: const KeeplyHome(),
    );
  }
}
class KeeplyHome extends StatefulWidget {
  const KeeplyHome({super.key});
  @override
  State<KeeplyHome> createState() => _KeeplyHomeState();
}
class _KeeplyHomeState extends State<KeeplyHome> {
  final _ws = WsController();
  int _currentIndex = 0;
  @override
  void initState() {
    super.initState();
    _initAgent();
  }
  Future<void> _initAgent() async {
    final dir = await getApplicationDocumentsDirectory();
    final config = WsConfig(
      url: 'wss://backend.keeply.app.br/ws/agent',
      deviceName: 'keeply-mobile',
      hostName: 'android-device',
      osName: 'android',
      identityDir: dir.path,
    );
    final identity = await _ws.identityController.ensureRegistered(config);
    config.agentId = identity.deviceId;
    await _ws.connect(config, identity);
  }
  @override
  void dispose() {
    _ws.dispose();
    super.dispose();
  }
  @override
  Widget build(BuildContext context) {
    final pages = [
      DashboardScreen(ws: _ws),
      TestScreen(ws: _ws),
    ];
    return Scaffold(
      body: pages[_currentIndex],
      bottomNavigationBar: BottomNavigationBar(
        currentIndex: _currentIndex,
        onTap: (i) => setState(() => _currentIndex = i),
        backgroundColor: const Color(0xFF1E293B),
        selectedItemColor: const Color(0xFF3B82F6),
        unselectedItemColor: const Color(0xFF64748B),
        type: BottomNavigationBarType.fixed,
        items: const [
          BottomNavigationBarItem(icon: Icon(Icons.dashboard), label: 'Dashboard'),
          BottomNavigationBarItem(icon: Icon(Icons.terminal), label: 'Teste'),
        ],
      ),
    );
  }
}
