@echo off
cd /d "%~dp0"
title Travel App - Schedule Weekly Backup

echo ========================================
echo   Scheduling Weekly Automatic Backup
echo ========================================
echo.
echo NOTE: Run this as Administrator if it fails.
echo       Right-click this file ^> "Run as administrator"
echo.

set "PYTHON=%~dp0venv\Scripts\python.exe"
set "SCRIPT=%~dp0weekly_backup.py"
set "TASK_NAME=SI2Tech_TravelApp_WeeklyBackup"

:: Check venv python exists (setup.bat must have been run first)
if not exist "%PYTHON%" (
    echo [ERROR] venv not found.
    echo Please run setup.bat first before scheduling the backup.
    pause
    exit /b 1
)

:: Check backup script exists
if not exist "%SCRIPT%" (
    echo [ERROR] weekly_backup.py not found in this folder.
    echo Make sure weekly_backup.py is in the same folder as this file.
    pause
    exit /b 1
)

:: Check .env exists
if not exist "%~dp0.env" (
    echo [ERROR] .env file not found.
    echo The backup script needs your .env file to connect to the database.
    pause
    exit /b 1
)

:: Remove old task silently if it exists (safe to re-run)
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

:: Schedule: every Sunday at 11:00 PM, silent, highest privileges
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\"" ^
  /sc WEEKLY ^
  /d SUN ^
  /st 23:00 ^
  /rl HIGHEST ^
  /f ^
  /ru "%USERNAME%"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to create scheduled task.
    echo Please right-click this file and choose "Run as administrator".
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Backup Scheduled Successfully!
echo ========================================
echo.
echo   Runs every : Sunday at 11:00 PM  (silent, no window)
echo   Script     : %SCRIPT%
echo   Log file   : %~dp0db_backups\backup_log.txt
echo.
echo   To verify: Open Task Scheduler ^> Task Scheduler Library
echo   Look for : %TASK_NAME%
echo.
pause
