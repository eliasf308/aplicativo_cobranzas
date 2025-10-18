<# 
Promecor - Menu integrado (PowerShell UI, PS 5.1 compatible)
- Tunel (cloudflared) start/stop + abrir web
- Enviar deudas ART (Florencia/Productores) con Celery
- Deteccion de Python que tenga Celery (auto) con opcion fija
#>

#region ========== CONFIG ==========
$ProjectDir = 'C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas'
$TunnelBat  = 'C:\Users\Promecor\Documents\Aplicativo cobranzas\start_cobranzas_named_tunnel.bat'
$AppUrl     = 'https://app.promecorcobranzas.com/'

# PIN: Python que sabemos que tiene Celery (segun tu prueba)
$PinnedPython = 'C:\Users\Promecor\Desktop\aplicativo_cobranzas\env\Scripts\python.exe'

# Si preferis autodeteccion sin usar el "pin", pone $false
$UsePinnedPython = $true
#endregion

#region ========== RENDER helpers (UI) ==========
function Show-Header {
    Clear-Host
    $title = "PROMECOR - COBRANZAS"
    Write-Host ("=" * ($title.Length + 8)) -ForegroundColor DarkCyan
    Write-Host ("   {0}   " -f $title) -ForegroundColor Cyan
    Write-Host ("=" * ($title.Length + 8)) -ForegroundColor DarkCyan
    ""
}

function Show-Status {
    param([string]$PythonExe)
    $cloud = Get-Process -Name cloudflared -ErrorAction SilentlyContinue
    $cel1  = Get-Process | Where-Object { $_.MainWindowTitle -eq 'Celery - florencia' }
    $cel2  = Get-Process | Where-Object { $_.MainWindowTitle -eq 'Celery - productores' }

    $tunnelStatus = if ($cloud) { 'ACTIVO' } else { 'NO detectado' }
    $tunnelColor  = if ($cloud) { 'Green' } else { 'DarkGray' }
    $cel1Status   = if ($cel1)  { 'activo' } else { 'no' }
    $cel1Color    = if ($cel1)  { 'Green' } else { 'DarkGray' }
    $cel2Status   = if ($cel2)  { 'activo' } else { 'no' }
    $cel2Color    = if ($cel2)  { 'Green' } else { 'DarkGray' }

    Write-Host ("Proyecto : {0}" -f $ProjectDir) -ForegroundColor Gray
    Write-Host ("Tunel    : {0}" -f $tunnelStatus) -ForegroundColor $tunnelColor
    Write-Host ("Celery F : {0}" -f $cel1Status) -ForegroundColor $cel1Color
    Write-Host ("Celery P : {0}" -f $cel2Status) -ForegroundColor $cel2Color
    Write-Host ("Python   : {0}" -f ($PythonExe ?? '(no detectado)')) -ForegroundColor Cyan
    ""
}

function Menu {
    Write-Host "[1] Iniciar tunel" -ForegroundColor White
    Write-Host "[2] Detener tunel" -ForegroundColor White
    Write-Host "[3] Enviar deudas ART" -ForegroundColor White
    Write-Host "[4] Detener envios (cerrar Celery)" -ForegroundColor White
    Write-Host "[5] Abrir web" -ForegroundColor White
    Write-Host "[6] Diagnostico" -ForegroundColor White
    Write-Host "[7] Salir" -ForegroundColor White
    ""
}
#endregion

#region ========== CORE: Python detection & actions ==========
function Get-PythonWithCelery {
    # 1) Pinned
    if ($UsePinnedPython -and (Test-Path $PinnedPython)) {
        try { & $PinnedPython -c "import celery" 2>$null; if ($LASTEXITCODE -eq 0) { return $PinnedPython } } catch {}
    }

    # 2) Local venvs
    $candidates = @()
    $candidates += (Join-Path $ProjectDir "env\Scripts\python.exe")
    $candidates += (Join-Path $ProjectDir ".venv\Scripts\python.exe")

    # 3) Conda envs tipicos
    $roots = @("C:\Users\Promecor\miniconda3\envs","C:\Users\Admin\Miniconda3\envs")
    foreach ($r in $roots) {
        if (Test-Path $r) {
            Get-ChildItem -Path $r -Directory | ForEach-Object {
                $candidates += (Join-Path $_.FullName "python.exe")
            }
        }
    }

    # 4) Python del PATH
    $inPath = (& where.exe python) 2>$null
    $candidates += $inPath

    # Test candidates
    foreach ($py in $candidates | Select-Object -Unique) {
        if (-not [string]::IsNullOrWhiteSpace($py) -and (Test-Path $py)) {
            try {
                & $py -c "import celery" 2>$null
                if ($LASTEXITCODE -eq 0) { return $py }
            } catch {}
        }
    }
    return $null
}

function Start-Tunnel {
    if (-not (Test-Path $TunnelBat)) {
        Write-Host "ERROR: No se encontro el script de tunel:" -ForegroundColor Red
        Write-Host "  $TunnelBat" -ForegroundColor Red
        Read-Host "Enter para volver" | Out-Null
        return
    }
    $cloud = Get-Process -Name cloudflared -ErrorAction SilentlyContinue
    if ($cloud) {
        Write-Host "Ya hay un cloudflared activo. No se lanza otro." -ForegroundColor Yellow
    } else {
        Start-Process -FilePath $TunnelBat
        Start-Sleep -Seconds 2
        Write-Host "Tunel lanzado." -ForegroundColor Green
    }
    Start-Process $AppUrl | Out-Null
}

function Stop-Tunnel {
    Get-Process -Name cloudflared -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "Tunel detenido (si estaba activo)." -ForegroundColor Green
}

function Start-Celery {
    param([ValidateSet('florencia','productores')][string]$Alias, [string]$PythonExe)
    Stop-Celery | Out-Null
    $cmd = @"
`$host.ui.RawUI.WindowTitle = 'Celery - $Alias';
Set-Location '$ProjectDir';
`$env:GMAIL_SENDER_ALIAS = '$Alias';
& '$PythonExe' -m celery -A cobranzas_project worker -l info --pool=solo
"@
    Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit","-NoLogo","-Command",$cmd | Out-Null
    Start-Process $AppUrl | Out-Null
    Write-Host "Celery - $Alias lanzado." -ForegroundColor Green
}

function Stop-Celery {
    Get-Process | Where-Object { $_.MainWindowTitle -in @('Celery - florencia','Celery - productores') } |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "Ventanas Celery cerradas (si habia)." -ForegroundColor DarkGray
}

function Show-Diagnostics {
    Show-Header
    $py = $Global:PythonExe
    if (-not $py) { $py = Get-PythonWithCelery }
    Write-Host "=== Diagnostico ===" -ForegroundColor Cyan
    Write-Host "Proyecto  : $ProjectDir" -ForegroundColor Gray
    Write-Host "TunnelBat : $TunnelBat" -ForegroundColor Gray
    Write-Host "URL       : $AppUrl" -ForegroundColor Gray
    if ($py) {
        Write-Host "Python    : $py" -ForegroundColor Cyan
        try { & $py -V } catch {}
        try { & $py -m celery --version } catch {}
    } else {
        Write-Host "[ERROR] No se encontro un Python con Celery." -ForegroundColor Red
        Write-Host "Sugerencia: ajusta `$PinnedPython o pone `$UsePinnedPython = `$false para auto-buscar." -ForegroundColor Yellow
    }
    ""
    Read-Host "Enter para volver" | Out-Null
}
#endregion

#region ========== LOOP ==========
# Detect once on start
$Global:PythonExe = if ($UsePinnedPython) { if (Test-Path $PinnedPython) { $PinnedPython } else { $null } } else { $null }
if (-not $Global:PythonExe) { $Global:PythonExe = Get-PythonWithCelery }

while ($true) {
    Show-Header
    Show-Status -PythonExe $Global:PythonExe
    Menu
    $opt = Read-Host "Elegi una opcion [1-7]"
    switch ($opt) {
        '1' { Start-Tunnel }
        '2' { Stop-Tunnel }
        '3' {
            # Submenu ART
            Show-Header
            Show-Status -PythonExe $Global:PythonExe
            Write-Host "ENVIAR DEUDAS ART" -ForegroundColor White
            Write-Host "  [1] Promecor (florencia)" -ForegroundColor White
            Write-Host "  [2] Productores" -ForegroundColor White
            Write-Host "  [3] Volver" -ForegroundColor White
            ""
            $sub = Read-Host "Elegi [1-3]"
            if ($sub -eq '1') {
                if (-not $Global:PythonExe) { $Global:PythonExe = Get-PythonWithCelery }
                if ($Global:PythonExe) { Start-Celery -Alias 'florencia' -PythonExe $Global:PythonExe }
                else { Write-Host "No hay Python con Celery." -ForegroundColor Red; Read-Host "Enter para volver" | Out-Null }
            } elseif ($sub -eq '2') {
                if (-not $Global:PythonExe) { $Global:PythonExe = Get-PythonWithCelery }
                if ($Global:PythonExe) { Start-Celery -Alias 'productores' -PythonExe $Global:PythonExe }
                else { Write-Host "No hay Python con Celery." -ForegroundColor Red; Read-Host "Enter para volver" | Out-Null }
            }
        }
        '4' { Stop-Celery; Read-Host "Enter para volver" | Out-Null }
        '5' { Start-Process $AppUrl | Out-Null }
        '6' { Show-Diagnostics }
        '7' { break }
        default { }
    }
}
#endregion
