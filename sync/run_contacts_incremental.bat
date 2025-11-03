@echo off
setlocal enableextensions enabledelayedexpansion

rem === Config ===
set "REPO_DIR=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
set "VENV_ACT=%REPO_DIR%\env\Scripts\activate.bat"
set "PY_SCRIPT=%REPO_DIR%\sync\sync_contacts_incremental.py"
set "LOG_DIR=%REPO_DIR%\logs"
set "RETRIES=3"
set "SLEEP_SECS=60"

rem === Timestamp para el log ===
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOGFILE=%LOG_DIR%\contacts_incremental_%TS%.log"

echo [%date% %time%] Iniciando incremental Contacts... > "%LOGFILE%"
pushd "%REPO_DIR%"
call "%VENV_ACT%" >> "%LOGFILE%" 2>&1

set /a ATTEMPT=1
set "EXIT_CODE=1"

:loop
echo [%date% %time%] Intento !ATTEMPT!/%RETRIES% >> "%LOGFILE%"
python -u "%PY_SCRIPT%" >> "%LOGFILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
if "%EXIT_CODE%"=="0" goto success
if !ATTEMPT! GEQ %RETRIES% goto end
echo [%date% %time%] Fallo intento !ATTEMPT! (ExitCode=%EXIT_CODE%). Esperando %SLEEP_SECS%s... >> "%LOGFILE%"
powershell -NoProfile -Command "Start-Sleep -Seconds %SLEEP_SECS%"
set /a ATTEMPT+=1
goto loop

:success
echo [%date% %time%] OK en intento !ATTEMPT!. >> "%LOGFILE%"

:end
echo [%date% %time%] Finalizado. ExitCode=%EXIT_CODE% >> "%LOGFILE%"
popd
exit /b %EXIT_CODE%
