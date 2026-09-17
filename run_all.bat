@echo off
REM Double-click me to run the whole sketchstream project.
REM Pass extra flags through, e.g.:  run_all.bat -Distributed
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_all.ps1" %*
echo.
pause
