@echo off
cd /d "C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"

REM asegurar carpeta de logs
if not exist "logs" mkdir "logs"

REM fecha y hora seguras para el nombre
set dt=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%
set tm=%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%
set tm=%tm: =0%

REM usar el Python del venv explícito
"C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas\env\Scripts\python.exe" -u sync\sync_contacts_incremental.py ^
  >> "logs\contacts_incremental_%dt%_%tm%.log" 2>&1
