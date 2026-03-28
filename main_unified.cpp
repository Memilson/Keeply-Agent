// main_unified.cpp
// Ponto de entrada unico que engloba CLI, Agent e CBT Daemon.
//
// Uso:
//   keeply.exe                          -> CLI (menu interativo)
//   keeply.exe backup / timeline / ...  -> CLI com subcomando
//   keeply.exe agent [--url ...] ...    -> Agent WebSocket
//   keeply.exe cbt --root <dir> ...     -> CBT Daemon

// --- inclui implementacao da CLI ----------------------------------------
#define KEEPLY_CLI_IMPLEMENTATION 1
// Renomeia o main() da CLI para keeply_cli_main()
#define main keeply_cli_main
#include "keeply.cpp"
#undef main
#undef KEEPLY_CLI_IMPLEMENTATION

// --- inclui implementacao do Agent --------------------------------------
// Renomeia o main() do agent para keeply_agent_main()
#define main keeply_agent_main
#include "agente_main.cpp"
#undef main

// --- inclui implementacao do CBT Daemon ---------------------------------
#define KEEPLY_DAEMON_PROGRAM 1
// Renomeia o main() do daemon para keeply_cbt_main()
#define main keeply_cbt_main
#include "Backup/Rastreamento/Windows/daemon.cpp"
#undef main
#undef KEEPLY_DAEMON_PROGRAM

// --- dispatcher principal -----------------------------------------------
#include <cstring>
#include <iostream>

int main(int argc, char** argv) {
    // Se chamado como "keeply agent ..." ou "keeply cbt ..."
    // descarta argv[1] e passa o restante para o sub-main.
    if (argc >= 2) {
        const char* mode = argv[1];

        if (std::strcmp(mode, "agent") == 0) {
            // Desloca os args: argv[0]="keeply_agent", argv[1..] = resto
            argv[1] = argv[0];
            return keeply_agent_main(argc - 1, argv + 1);
        }

        if (std::strcmp(mode, "cbt") == 0) {
            argv[1] = argv[0];
            return keeply_cbt_main(argc - 1, argv + 1);
        }
    }

    // Sem argumentos ou argumento desconhecido: roda o agent diretamente
    return keeply_agent_main(argc, argv);
}
