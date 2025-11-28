#!/usr/bin/env bash
curl -s -X POST http://127.0.0.1:9000/api/mdirs/login \
  -H 'Content-Type: application/json' \
  -d '{"password":"1982567"}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])'
