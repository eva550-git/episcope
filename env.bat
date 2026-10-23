@echo off
set "JAVA_HOME=C:\Program Files\Java\jdk-21"
set "PATH=%JAVA_HOME%\bin;%PATH%"
call "%~dp0.venv\Scripts\activate.bat"
echo Environment ready.