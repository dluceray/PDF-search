#!/usr/bin/env bash
set -euo pipefail

login() {
  curl -s -X POST http://127.0.0.1:9000/api/mdirs/login \
    -H 'Content-Type: application/json' \
    -d '{"password":"1982567"}'
}

TOK=$(login | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')

python3 - <<'PY' | curl -s -X POST "http://127.0.0.1:9000/api/mdirs/reload" \
  -H "Content-Type: application/json" -H "X-Auth: '"$TOK"'" -d @- >/dev/null || true
import os, json
roots=set()
for dp, dn, fn in os.walk('/data/contracts'):
    if 'index.xlsx' in fn or '目录.xlsx' in fn:
        roots.add(dp)
schema={r:{"序号":"contract_no","签订途径":"sign_method"} for r in roots}
print(json.dumps({
  "roots": sorted(roots) or ["/data/contracts/2024"],
  "excel_patterns": ["index.xlsx","目录.xlsx"],
  "allowed_exts": [".pdf"],
  "schema_hints": schema
}, ensure_ascii=False))
PY
