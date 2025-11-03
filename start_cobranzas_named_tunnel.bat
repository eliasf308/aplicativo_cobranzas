@echo off
setlocal EnableExtensions

rem ==== Directorio del script/proyecto ====
set "PROJECT_DIR=%~dp0"

rem ==== Python del entorno virtual (relativo al proyecto) ====
set "VENV_PY=%PROJECT_DIR%env\Scripts\python.exe"

rem ==== Cloudflared (opcional; ajusta si lo usas) ====
set "CLOUDFLARED=C:\cloudflared\cloudflared.exe"
set "TUNNEL_ID=b9e384ec-7cbc-4cd9-a47b-e2c78557e149"

rem ==== Ir al proyecto ====
cd /d "%PROJECT_DIR%"

rem ==== Chequeos b?sicos ====
if not exist "%PROJECT_DIR%manage.py" (
  echo [ERROR] No se encontro manage.py en: %PROJECT_DIR%
  pause
  exit /b 1
)

if not exist "%VENV_PY%" (
  echo [ERROR] No se encontro el Python del entorno: %VENV_PY%
  echo Sugerencias:
  echo   - Crear/rehacer el venv:  python -m venv "%PROJECT_DIR%env"
  echo   - O ajustar la variable VENV_PY a la ruta correcta.
  pause
  exit /b 1
)

rem ==== Levantar Django con el Python del venv ====
start "Django server" cmd /k ""%VENV_PY%" manage.py runserver 127.0.0.1:8000"

rem ==== Levantar Cloudflare Tunnel (opcional) ====
if exist "%CLOUDFLARED%" (
  "%CLOUDFLARED%" tunnel run %TUNNEL_ID%
) else (
  echo [ADVERTENCIA] No se encontro cloudflared en: %CLOUDFLARED%
  echo Instalalo o ajusta la ruta si necesitas el acceso externo.
  pause
)

endlocal
