#!/usr/bin/env bash
set -Eeuo pipefail

LOCK="/var/run/update-all.lock"
PDF="/root/pdfsearch/bin/append_pdfs.py"
XLS="/root/pdfsearch/bin/ingest_excels.py"
VENV_PY="/root/pdfsearch/venv/bin/python"

cleanup(){ rm -f "$LOCK" || true; }
trap cleanup EXIT

# 轻量记录，便于回溯
{
  echo "[update_all] $(date +%F' '%T) PATH=$PATH"
  command -v python3 >/dev/null 2>&1 && echo "[update_all] python3=$(command -v python3)"
  echo "[update_all] venvpy=$VENV_PY"
  echo "[update_all] whoami=$(whoami)"
} >>/tmp/update_all.log

if [ -e "$LOCK" ]; then
  LOCK_PID="$(cat "$LOCK" 2>/dev/null || true)"
  if [ -n "$LOCK_PID" ] && kill -0 "$LOCK_PID" >/dev/null 2>&1; then
    echo '{"ok":false,"error":"another update running"}'; exit 1
  fi
  rm -f "$LOCK"
fi
echo "$$" >"$LOCK"

# 确保目录存在
mkdir -p /data/add_pending /data/add_done /data/add_error

# 1) 合并 to*.pdf（延续原行为，优先可执行；否则用 venv python）
if [ -f "$PDF" ]; then
  if [ -x "$PDF" ]; then "$PDF" || true; else "$VENV_PY" "$PDF" || true; fi
fi

# 2) 入库 Excel（显式传参 + 自动探测表头）
"$VENV_PY" "$XLS" \
  --contracts-root /data/contracts \
  --pending-dir   /data/add_pending \
  --done-dir      /data/add_done \
  --error-dir     /data/add_error \
  --header-row    auto \
  --summary-out   /tmp/ingest.summary.json \
  || true

echo '{"ok":true,"msg":"pdf merged then xlsx ingested"}'
