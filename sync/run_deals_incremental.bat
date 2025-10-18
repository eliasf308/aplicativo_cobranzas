@echo off
cd /d "C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"

if not exist "logs" mkdir "logs"

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyyMMdd_HHmmss')"') do set STAMP=%%i

"C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas\env\Scripts\python.exe" -u sync\sync_deals_incremental.py ^
  >> "logs\deals_incremental_%STAMP%.log" 2>&1
