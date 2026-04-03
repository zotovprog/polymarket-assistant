#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[safety] checking tracked sensitive files..."
tracked_forbidden="$(git ls-files | grep -E '(^\.env$|^\.env\..+$|^\.web_access_key$)' | grep -v '^\.env\.example$' || true)"
if [[ -n "${tracked_forbidden}" ]]; then
  echo "[safety] forbidden tracked env/secret files detected:"
  echo "${tracked_forbidden}"
  exit 1
fi

echo "[safety] checking known leaked literal patterns..."
if rg -n "damage-hammer-depart|mongodb://gen_user|147\\.45\\.146\\.98" -S . --glob '!**/.git/**' --glob '!tools/check_repo_safety.sh' >/dev/null; then
  echo "[safety] known leaked literals found in working tree"
  rg -n "damage-hammer-depart|mongodb://gen_user|147\\.45\\.146\\.98" -S . --glob '!**/.git/**' --glob '!tools/check_repo_safety.sh'
  exit 1
fi

echo "[safety] checking required ignore rules..."
for entry in ".env" ".env.*" ".web_access_key" "audit/" "data/" "tasks/" "codex_outputs/"; do
  if ! grep -qxF "${entry}" .gitignore; then
    echo "[safety] missing .gitignore rule: ${entry}"
    exit 1
  fi
done

echo "[safety] OK"
