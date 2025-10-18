@echo off
setlocal ENABLEDELAYEDEXPANSION
title Promecor - Cobranzas (Menu BAT estetico)
color 1F

rem =====================
rem CONFIG
rem =====================
set "PROJECT_DIR=C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
set "TUNNEL_BAT=C:\Users\Promecor\Documents\Aplicativo cobranzas\start_cobranzas_named_tunnel.bat"
set "APP_URL=https://app.promecorcobranzas.com/"
rem Python bueno detectado por vos:
set "CUSTOM_PYTHON_EXE=C:\Users\Promecor\Desktop\aplicativo_cobranzas\env\Scripts\python.exe"

call :detect_python

:menu
cls
echo.
echo ============================
echo    PROMECOR - COBRANZAS
echo ============================
echo.
call :status_info
echo.
echo [1] Iniciar tunel/servidor
echo [2] Detener tunel/servidor
echo [3] Enviar deudas ART
echo [4] Detener envios (cerrar Celery)
echo [5] Abrir web
echo [6] Diagnostico
echo [7] Salir
echo.
choice /C 1234567 /N /M "Elegir [1-7]: "
if errorlevel 7 goto end
if errorlevel 6 goto diag
if errorlevel 5 goto open_web
if errorlevel 4 goto stop_celery
if errorlevel 3 goto submenu_art
if errorlevel 2 goto stop_tunnel
if errorlevel 1 goto start_tunnel
goto menu

:submenu_art
cls
echo.
echo ------- ENVIAR DEUDAS ART -------
echo [1] Promecor (alias: florencia)
echo [2] Productores (alias: productores)
echo [3] Volver
echo ---------------------------------
choice /C 123 /N /M "Elegir [1-3]: "
if errorlevel 3 goto menu
if errorlevel 2 goto start_prod
if errorlevel 1 goto start_flor
goto submenu_art

:start_tunnel
cls
echo Iniciando tunel/servidor...
if not exist "%TUNNEL_BAT%" (
  echo ERROR: No se encontro TUNNEL_BAT:
  echo   %TUNNEL_BAT%
  echo Edita la variable al inicio del .bat y probalo de nuevo.
  echo.
  pause
  goto menu
)
tasklist /FI "IMAGENAME eq cloudflared.exe" | find /I "cloudflared.exe" >nul
if %ERRORLEVEL%==0 (
  echo Ya hay un cloudflared.exe activo. No se lanza otro.
) else (
  start "Tunnel/Server" "%TUNNEL_BAT%"
  echo Lanzado. Espera unos segundos...
)
call :open_browser_quiet
pause
goto menu

:stop_tunnel
cls
echo Deteniendo tunel/servidor...
taskkill /IM cloudflared.exe /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Django server" /T /F >nul 2>&1
echo Listo.
pause
goto menu

:start_flor
call :stop_celery_silent
cls
echo Iniciando Celery para FLORENCIA...
start "Celery - florencia" cmd /k "cd /d ""%PROJECT_DIR%"" && set GMAIL_SENDER_ALIAS=florencia && ""%PYTHON_EXE%"" -m celery -A cobranzas_project worker -l info --pool=solo"
call :open_browser_quiet
echo Listo. Deja abierta la ventana "Celery - florencia".
pause
goto menu

:start_prod
call :stop_celery_silent
cls
echo Iniciando Celery para PRODUCTORES...
start "Celery - productores" cmd /k "cd /d ""%PROJECT_DIR%"" && set GMAIL_SENDER_ALIAS=productores && ""%PYTHON_EXE%"" -m celery -A cobranzas_project worker -l info --pool=solo"
call :open_browser_quiet
echo Listo. Deja abierta la ventana "Celery - productores".
pause
goto menu

:stop_celery
cls
echo Cerrando Celery (si esta abierto)...
taskkill /FI "WINDOWTITLE eq Celery - productores" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Celery - florencia"  /T /F >nul 2>&1
echo Listo.
pause
goto menu

:stop_celery_silent
taskkill /FI "WINDOWTITLE eq Celery - productores" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Celery - florencia"  /T /F >nul 2>&1
exit /b 0

:open_web
call :open_browser_quiet
goto menu

:open_browser_quiet
start "" "%APP_URL%"
exit /b 0

:diag
cls
echo ===== Diagnostico =====
echo Proyecto:   %PROJECT_DIR%
echo Tunnel bat: %TUNNEL_BAT%
echo URL:        %APP_URL%
echo.
echo Python seleccionado: %PYTHON_EXE%
echo (ruta real de ese Python):
"%PYTHON_EXE%" -c "import sys; print(sys.executable)"
echo.
echo Celery version (desde Python seleccionado):
"%PYTHON_EXE%" -m celery --version
echo.
tasklist /FI "IMAGENAME eq cloudflared.exe"
tasklist /FI "WINDOWTITLE eq Celery - productores"
tasklist /FI "WINDOWTITLE eq Celery - florencia"
echo.
pause
goto menu

:status_info
tasklist /FI "IMAGENAME eq cloudflared.exe" | find /I "cloudflared.exe" >nul
if %ERRORLEVEL%==0 (
  echo Estado tunel: ACTIVO
) else (
  echo Estado tunel: NO detectado
)
tasklist /FI "WINDOWTITLE eq Celery - productores" | find /I "Celery - productores" >nul
if %ERRORLEVEL%==0 echo Celery PRODUCTORES: activo
tasklist /FI "WINDOWTITLE eq Celery - florencia" | find /I "Celery - florencia" >nul
if %ERRORLEVEL%==0 echo Celery FLORENCIA:  activo
echo Python: %PYTHON_EXE%
exit /b 0

:detect_python
set "PYTHON_EXE="
if defined CUSTOM_PYTHON_EXE if exist "%CUSTOM_PYTHON_EXE%" set "PYTHON_EXE=%CUSTOM_PYTHON_EXE%"
if "%PYTHON_EXE%"=="" if exist "%PROJECT_DIR%\env\Scripts\python.exe" set "PYTHON_EXE=%PROJECT_DIR%\env\Scripts\python.exe"
if "%PYTHON_EXE%"=="" if exist "%PROJECT_DIR%\.venv\Scripts\python.exe" set "PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe"
if "%PYTHON_EXE%"=="" set "PYTHON_EXE=python"
exit /b 0

:end
endlocal
exit /b 0
