@echo off
setlocal
REM Re-crea un entorno virtual desde cero y verifica que el proyecto corre.
REM Uso: doble clic o ejecutar desde la raíz del proyecto.

set "ENV_DIR=env"
set "PY=python"

echo [1/5] Eliminando entorno anterior (si existe)...
if exist "%ENV_DIR%" rmdir /s /q "%ENV_DIR%"

echo [2/5] Creando entorno virtual...
%PY% -m venv "%ENV_DIR%"
if errorlevel 1 (
  echo [ERROR] Fallo al crear el entorno virtual.
  pause
  exit /b 1
)

echo [3/5] Activando entorno...
call "%ENV_DIR%\Scripts\activate.bat"

echo [4/5] Actualizando pip...
python -m pip install --upgrade pip

echo [4b/5] Instalando requirements (runtime)...
pip install -r requirements.txt
if errorlevel 1 (
  echo [ERROR] Fallo al instalar requirements.txt
  pause
  exit /b 1
)

if exist requirements-dev.txt (
  echo [4c/5] Instalando requirements (dev)...
  pip install -r requirements-dev.txt
)

echo [5/5] Verificando proyecto...
python manage.py check
if errorlevel 1 (
  echo [ERROR] manage.py check falló.
  pause
  exit /b 1
)

echo Todo OK. Para iniciar: python manage.py runserver
pause
endlocal
