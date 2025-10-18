@echo off
setlocal
cd /d "C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"

rem Timestamp robusto (independiente del locale)
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i

set "PY=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas\env\Scripts\python.exe"
if not exist "%PY%" (
  echo [%date% %time%] ERROR: Python no encontrado en "%PY%" >> "logs\deals_daily_%TS%.log"
  exit /b 1
)

"%PY%" -u sync\sync_deals_incremental.py >> "logs\deals_daily_%TS%.log" 2>&1
exit /b %ERRORLEVEL%
