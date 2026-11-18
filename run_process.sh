#!/usr/bin/env bash
# Starts a background process (calc service, cron, ...) through run_process.py.
# The server launches this script with: nohup bash run_process.sh <args>
cd "$(dirname "$0")"
if [ -x .venv/bin/python ]; then
    PY=.venv/bin/python
else
    PY=python3
fi
exec "$PY" run_process.py "$@"
