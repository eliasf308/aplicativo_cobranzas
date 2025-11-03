@echo off
setlocal enabledelayedexpansion

rem === Rutas ===
set "REPO_DIR=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
set "VENV_ACT=%REPO_DIR%\env\Scripts\activate.bat"
set "PY_SCRIPT=%REPO_DIR%\sync\sync_accounts_incremental.py"
set "LOG_DIR=%REPO_DIR%\logs"

rem === Timestamp para el log ===
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOGFILE=%LOG_DIR%\accounts_incremental_%TS%.log"

echo [%date% %time%] Iniciando incremental Accounts... > "%LOGFILE%"
pushd "%REPO_DIR%"

rem === Activar venv y ejecutar ===
call "%VENV_ACT%" >> "%LOGFILE%" 2>&1
python -u "%PY_SCRIPT%" >> "%LOGFILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%date% %time%] Finalizado. ExitCode=%EXIT_CODE% >> "%LOGFILE%"
popd
exit /b %EXIT_CODE%
