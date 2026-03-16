@echo off
cd /d "%~dp0"
title Travel App - Debug Launcher

:: 1. Load .env variables (Visual check)
if exist ".env" (
    echo [.env found] Loading keys...
) else (
    echo [WARNING] .env missing. App may crash if keys are required.
)

:: 2. Launch with venv python if available, otherwise system python
echo Starting Streamlit...
if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe -m streamlit run finalfile.py --server.port 8503
) else (
    python -m streamlit run finalfile.py --server.port 8503
)

:: 3. Keep window open if the app crashes
if %errorlevel% neq 0 (
    echo.
    echo [CRASH DETECTED] The app stopped with exit code %errorlevel%.
    echo Check the error message above to see which library is missing.
    pause
)
