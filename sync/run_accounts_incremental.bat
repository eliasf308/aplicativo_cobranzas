@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >NUL

REM --- Paths del proyecto ---
set "PROJ=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
set "PY=%PROJ%\env\Scripts\python.exe"
set "SCRIPT=%PROJ%\sync\sync_accounts_incremental.py"
set "LOGDIR=%PROJ%\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

REM --- Timestamp robusto ---
for /f %%I in ('powershell -NoProfile -Command "(Get-Date -Format yyyyMMdd_HHmmss)"') do set "TS=%%I"
set "LOG=%LOGDIR%\accounts_incremental_%TS%.log"

REM --- Single instance lock ---
set "LOCK=%LOGDIR%\accounts.lock"
if exist "%LOCK%" (
  echo [%date% %time%] Otro proceso de Accounts ya está en curso. Saliendo...>>"%LOG%"
  exit /b 0
)
type NUL > "%LOCK%"

pushd "%PROJ%" >NUL
echo [%date% %time%] Iniciando Accounts...>"%LOG%"
echo PY="%PY%" >>"%LOG%"
echo SCRIPT="%SCRIPT%" >>"%LOG%"

"%PY%" "%SCRIPT%" >>"%LOG%" 2>&1
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
  echo [%date% %time%] Fallo ExitCode=%EC%. Reintento en 60s...>>"%LOG%"
  timeout /t 60 /nobreak >NUL
  "%PY%" "%SCRIPT%" >>"%LOG%" 2>&1
  set "EC=%ERRORLEVEL%"
)

if not defined EC set "EC=0"
echo [%date% %time%] Finalizado Accounts. ExitCode=!EC!>>"%LOG%"

popd >NUL
del "%LOCK%" >NUL 2>&1

REM --- Rotación de logs (>30 días)
forfiles /P "%LOGDIR%" /M "accounts_incremental_*.log" /D -30 /C "cmd /c del @file" 2>NUL

exit /b !EC!
