#!/usr/bin/env bash
set -euo pipefail
for target in "$HOME/.local/share/keeply" "$HOME/.local/state/keeply"; do
  if [ -d "$target" ]; then
    rm -rf "$target"
    echo "apagado: $target"
  else
    echo "nao existe: $target"
  fi
done
