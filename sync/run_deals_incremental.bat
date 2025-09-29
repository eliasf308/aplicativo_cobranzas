@echo off
setlocal
cd /d C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas

REM activar entorno
call env\Scripts\activate.bat

REM ejecutar y loguear salida
python sync\sync_deals_incremental.py >> sync\_state\deals_sync.log 2>&1

endlocal
