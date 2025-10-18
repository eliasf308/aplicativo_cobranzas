@echo off
cd /d "C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"

REM asegurar carpeta de logs
if not exist "logs" mkdir "logs"

REM armar fecha y hora seguras para nombre de archivo
set dt=%DATE:~-4%%DATE:~3,2%%DATE:~0,2%
set tm=%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%
set tm=%tm: =0%

REM usar el Python del venv explícito + salida sin buffer
"C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas\env\Scripts\python.exe" -u sync\sync_accounts_incremental.py ^
  >> "logs\accounts_incremental_%dt%_%tm%.log" 2>&1
