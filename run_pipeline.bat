@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo Python is required. Install Python 3.10 or newer and retry.
    exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
    python -m venv .venv
    if errorlevel 1 exit /b 1
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 exit /b 1
)

if not exist ".env" (
    ".venv\Scripts\python.exe" -c "from pathlib import Path; import secrets; Path('.env').write_text('PII_SALT='+secrets.token_urlsafe(48)+chr(10), encoding='utf-8')"
    if errorlevel 1 exit /b 1
)

if not defined JAVA_HOME if exist "%ProgramFiles%\Java\jdk-1.8\bin\java.exe" set "JAVA_HOME=%ProgramFiles%\Java\jdk-1.8"
if defined JAVA_HOME set "PATH=%JAVA_HOME%\bin;%PATH%"

if not defined SF set "SF=0.10"
if not defined SEED set "SEED=42"
".venv\Scripts\python.exe" -m pipeline.run_pipeline --sf "%SF%" --seed "%SEED%" --serve
exit /b %errorlevel%
