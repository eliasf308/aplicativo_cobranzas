@echo off
cd /d "C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas"
if not exist "logs" exit /b 0

REM borra logs con más de 30 días (cualquier *.log en la carpeta logs)
powershell -NoProfile -Command ^
  "$cut=(Get-Date).AddDays(-30); $files=Get-ChildItem 'logs' -Filter '*.log' -ErrorAction SilentlyContinue | ? {$_.LastWriteTime -lt $cut}; $n=$files.Count; if($n){$files | Remove-Item -Force -ErrorAction SilentlyContinue; Write-Host ('Eliminados ' + $n + ' logs viejos.')} else {Write-Host 'No hay logs para eliminar.'}"
