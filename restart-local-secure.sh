#!/bin/bash
set -euo pipefail
pkill -f "build/keeply" && echo "Processo encerrado." || echo "Nenhum processo keeply encontrado."
sleep 1
args=(./build/keeply --foreground --url wss://127.0.0.1:8443/ws/agent --root /home/angelo/Documentos --device 4US-N0001)
if [ "${KEEPLY_INSECURE_TLS:-1}" = "1" ]; then
  args+=(--insecure-tls)
fi
exec "${args[@]}"
