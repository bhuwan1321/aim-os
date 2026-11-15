@echo off
title AIM OS
cd /d "%~dp0"
where py >nul 2>nul && (py -3 aim.py %* & goto :end)
where python >nul 2>nul && (python aim.py %* & goto :end)
echo Python 3 is required: https://www.python.org/downloads/
:end
if "%1"=="" pause
