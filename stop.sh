#!/usr/bin/env sh
cd "$(dirname "$0")" || exit 1
exec "$(command -v python3 || command -v python)" aim.py stop
