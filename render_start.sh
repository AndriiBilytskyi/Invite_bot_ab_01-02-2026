#!/usr/bin/env bash
set -eu
set -o pipefail

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

mkdir -p /var/data
cp -f "$SECRET_DIR/sessions.json" /var/data/sessions.json

# decode *.b64 if present, else copy *.session
if ls "$SECRET_DIR"/session*.b64 >/dev/null 2>&1; then
  echo "[boot] decoding session*.b64 -> /var/data/*.session"
  for f in "$SECRET_DIR"/session*.b64; do
    base="$(basename "$f" .b64)"
    echo "[boot] decode $base"
    python - "$f" "/var/data/${base}.session" <<'PY'
import base64, re, sys
src, dst = sys.argv[1], sys.argv[2]
b = open(src, "rb").read()
b = re.sub(rb"[^A-Za-z0-9+/=]", b"", b)
b += b"=" * ((-len(b)) % 4)
open(dst, "wb").write(base64.b64decode(b))
PY
  done
elif ls "$SECRET_DIR"/session*.session >/dev/null 2>&1; then
  echo "[boot] copying session*.session -> /var/data"
  cp -f "$SECRET_DIR"/session*.session /var/data/
else
  echo "[boot] WARNING: no session files found in secrets"
fi

chmod 600 /var/data/sessions.json || true
chmod 600 /var/data/session*.session 2>/dev/null || true

echo "[boot] /var/data contents:"
ls -la /var/data | sed -n "1,200p"

exec python -u multi_invite_rotator.py
