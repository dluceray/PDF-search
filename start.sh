#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec uvicorn app:app --host 0.0.0.0 --port 9000 --workers 1
