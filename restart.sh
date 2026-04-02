#!/bin/bash
pkill -f "build/keeply" && echo "Processo encerrado." || echo "Nenhum processo keeply encontrado."
sleep 1
exec ./build/keeply --foreground --url ws://127.0.0.1:8081/ws/agent --root /home/angelo/Documentos --device 4US-N0001
