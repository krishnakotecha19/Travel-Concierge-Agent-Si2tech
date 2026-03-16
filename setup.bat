@echo off
cd /d "%~dp0"
title Travel App - Full Clean Install

echo ========================================
echo   STEP 1: Checking for Python
echo ========================================

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python not found. Please install Python 3.12 from:
    echo https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)

python --version
echo Python found.

echo.
echo ========================================
echo   STEP 2: Deleting EVERYTHING old
echo ========================================

:: Delete venv
if exist "venv" (
    echo Deleting old venv...
    rmdir /s /q venv
)

:: Delete all __pycache__ folders
for /d /r %%d in (__pycache__) do (
    if exist "%%d" rmdir /s /q "%%d"
)

:: Delete old cache files
if exist "flight_iata_cache.json" del /f /q flight_iata_cache.json
if exist "mmt_city_cache.json" del /f /q mmt_city_cache.json
if exist "search_history.json" del /f /q search_history.json
if exist "scraped_receipts.json" del /f /q scraped_receipts.json

echo Old files cleaned.

echo.
echo ========================================
echo   STEP 3: Fresh Install
echo ========================================

:: Create fresh venv
echo Creating virtual environment...
python -m venv venv

if not exist "venv\Scripts\python.exe" (
    echo.
    echo [WARNING] venv creation failed. Using system Python.
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    goto :db_setup
)

echo Installing all dependencies from scratch...
venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\python.exe -m pip install -r requirements.txt

:db_setup
echo.
echo ========================================
echo   STEP 4: Setting Up Database
echo ========================================

if not exist ".env" (
    echo.
    echo [ERROR] .env file not found!
    echo Create a .env file with your database details and run setup.bat again.
    pause
    exit /b 1
)

if not exist "setup_database.py" (
    echo [ERROR] setup_database.py not found.
    pause
    exit /b 1
)

echo Running database setup...
if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe setup_database.py
) else (
    python setup_database.py
)

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Database setup failed!
    echo Check that PostgreSQL is running and .env has correct DB_PASSWORD.
    pause
    exit /b 1
)

echo.
echo ========================================
echo   ALL DONE! Fresh Install Complete.
echo ========================================
echo.
echo  Run launch.bat to start the app.
echo.
pause
