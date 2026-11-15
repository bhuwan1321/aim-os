#!/usr/bin/env sh
# AIM OS launcher (macOS / Linux)
cd "$(dirname "$0")" || exit 1
PY=$(command -v python3 || command -v python) || { echo "Python 3 is required: https://www.python.org/downloads/"; exit 1; }
[ $# -eq 0 ] && set -- start
exec "$PY" aim.py "$@"
