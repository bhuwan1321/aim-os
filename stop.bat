@echo off
cd /d "%~dp0"
where py >nul 2>nul && (py -3 aim.py stop & goto :end)
python aim.py stop
:end
pause
