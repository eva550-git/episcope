@echo off
setlocal
cd /d "%~dp0"

if defined JAVA_HOME (
    if not exist "%JAVA_HOME%\bin\java.exe" set "JAVA_HOME="
)

if defined JAVA_HOME (
    "%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr /R /C:"version .*1\.8" /C:"version .*11\." /C:"version .*17\." >nul
    if errorlevel 1 set "JAVA_HOME="
)

if not defined JAVA_HOME if exist "%ProgramFiles%\Java\jdk-1.8\bin\java.exe" set "JAVA_HOME=%ProgramFiles%\Java\jdk-1.8"

if not defined JAVA_HOME (
    echo PySpark 3.5 requires Java 8, 11, or 17. Set JAVA_HOME to a supported JDK and retry.
    exit /b 1
)

set "PATH=%JAVA_HOME%\bin;%PATH%"
echo Using Java from "%JAVA_HOME%"
if /i "%~1"=="--check-java" exit /b 0

where py >nul 2>nul
if errorlevel 1 (
    echo The Python launcher is required. Install Python 3.11 and retry.
    exit /b 1
)

if not exist ".venv311\Scripts\python.exe" (
    py -3.11 -m venv .venv311
    if errorlevel 1 exit /b 1
    ".venv311\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 exit /b 1
)

".venv311\Scripts\python.exe" -c "import sys; sys.exit(0 if sys.version_info[:2] == (3, 11) else 1)"
if errorlevel 1 (
    echo The .venv311 environment must use Python 3.11. Recreate it with py -3.11 -m venv .venv311.
    exit /b 1
)

if not exist ".env" (
    ".venv311\Scripts\python.exe" -c "from pathlib import Path; import secrets; Path('.env').write_text('PII_SALT='+secrets.token_urlsafe(48)+chr(10), encoding='utf-8')"
    if errorlevel 1 exit /b 1
)

if not defined SF set "SF=0.10"
if not defined SEED set "SEED=42"
".venv311\Scripts\python.exe" -m pipeline.run_pipeline --sf "%SF%" --seed "%SEED%" --serve
exit /b %errorlevel%
