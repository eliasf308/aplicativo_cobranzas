@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >NUL

REM --- Paths del proyecto ---
set "PROJ=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
set "PY=%PROJ%\env\Scripts\python.exe"
set "SCRIPT=%PROJ%\sync\sync_contacts_incremental.py"
set "LOGDIR=%PROJ%\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

REM --- Log con timestamp YYYYMMDD_HHMMSS ---
for /f "tokens=1-3 delims=/:. " %%a in ("%date% %time%") do (
  set "YYMMDD=%%c%%b%%a"
)
set "HH=%time:~0,2%"
if "%HH:~0,1%"==" " set "HH=0%HH:~1,1%"
set "TS=%YYMMDD%_%HH%%time:~3,2%%time:~6,2%"
set "LOG=%LOGDIR%\contacts_incremental_%TS%.log"

REM --- Single instance lock ---
set "LOCK=%LOGDIR%\contacts.lock"
if exist "%LOCK%" (
  echo [%date% %time%] Otro proceso de Contacts ya está en curso. Saliendo...>>"%LOG%"
  exit /b 0
)
type NUL > "%LOCK%"

pushd "%PROJ%" >NUL

REM --- Arranque ---
echo [%date% %time%] Iniciando Contacts...>"%LOG%"
echo PY="%PY%" >>"%LOG%"
echo SCRIPT="%SCRIPT%" >>"%LOG%"

REM --- Ejecutar (1 intento + 1 reintento si falla) ---
"%PY%" "%SCRIPT%" >>"%LOG%" 2>&1
set "EC=%ERRORLEVEL%"

if not "%EC%"=="0" (
  echo [%date% %time%] Fallo ExitCode=%EC%. Reintento en 60s...>>"%LOG%"
  timeout /t 60 /nobreak >NUL
  "%PY%" "%SCRIPT%" >>"%LOG%" 2>&1
  set "EC=%ERRORLEVEL%"
)

echo [%date% %time%] Finalizado Contacts. ExitCode=%EC%>>"%LOG%"

del "%LOCK%" >NUL 2>&1
popd >NUL
exit /b %EC%

