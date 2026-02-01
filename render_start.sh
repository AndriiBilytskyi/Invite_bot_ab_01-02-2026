#!/usr/bin/env bash
set -eu
set -o pipefail

# 1) найти директорию секретов
SECRET_DIR=""
for d in /etc/secrets /run/secrets /etc/render/secrets; do
  if [ -f "$d/sessions.json" ]; then
    SECRET_DIR="$d"
    break
  fi
done

echo "[boot] secret dir: ${SECRET_DIR:-NOT_FOUND}"
if [ -z "$SECRET_DIR" ]; then
  echo "[boot] ERROR: sessions.json not found"
  exit 1
fi

# 2) подготовить /var/data
mkdir -p /var/data
cp -f "$SECRET_DIR/sessions.json" /var/data/sessions.json

# 3) если есть session*.b64 — декодируем устойчиво; иначе копируем session*.session
shopt -s nullglob
b64s=("$SECRET_DIR"/session*.b64)

if [ "${#b64s[@]}" -gt 0 ]; then
  echo "[boot] decoding session*.b64 -> /var/data/*.session"
  for f in "${b64s[@]}"; do
    base="$(basename "$f" .b64)"
    echo "[boot] decode $base"
    python - "$f" "/var/data/${base}.session" <<'PY'
import base64, re, sys
src, dst = sys.argv[1], sys.argv[2]
b = open(src, "rb").read()
# вычищаем все “мусорные” символы, оставляя только base64-алфавит
b = re.sub(rb"[^A-Za-z0-9+/=]", b"", b)
# дополняем padding
b += b"=" * ((-len(b)) % 4)
open(dst, "wb").write(base64.b64decode(b))
PY
  done
else
  sess=("$SECRET_DIR"/session*.session)
  if [ "${#sess[@]}" -gt 0 ]; then
    echo "[boot] copying session*.session -> /var/data"
    cp -f "${sess[@]}" /var/data/
  else
    echo "[boot] WARNING: no session files found in secrets"
  fi
fi

chmod 600 /var/data/sessions.json || true
chmod 600 /var/data/session*.session 2>/dev/null || true

echo "[boot] /var/data contents:"
ls -la /var/data | sed -n "1,200p"

exec python -u multi_invite_rotator.py

