#!/usr/bin/env python3
"""CLI for the AIM project scanner:  python tools/aim_scanner.py --path <project> --out result.json"""
import os, runpy, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "agents", "research"))
runpy.run_module("scanner", run_name="__main__")
