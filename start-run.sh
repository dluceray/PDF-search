
#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
if [ -d "venv" ]; then source venv/bin/activate; fi
# pip install skipped on normal boots
exec python app.py
