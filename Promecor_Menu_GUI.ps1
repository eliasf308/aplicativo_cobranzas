<# 
Promecor - Cobranzas (PowerShell GUI - WPF, PS 5.1 compatible)
- Tunel (cloudflared) start/stop + abrir web
- Enviar deudas ART (Florencia/Productores) con Celery
- Deteccion de Python con Celery (pin + fallback)
ASCII only (sin acentos), para evitar problemas de codificacion.
#>

#region CONFIG
$ProjectDir   = 'C:\Users\Promecor\Documents\Promecor\aplicativo_cobranzas'
$TunnelBat    = 'C:\Users\Promecor\Documents\Aplicativo cobranzas\start_cobranzas_named_tunnel.bat'
$AppUrl       = 'https://app.promecorcobranzas.com/'
# Python que sabemos que tiene Celery:
$PinnedPython = 'C:\Users\Promecor\Desktop\aplicativo_cobranzas\env\Scripts\python.exe'
$UsePinned    = $true
#endregion

#region Helpers: detection & actions
function Get-PythonWithCelery {
    # 1) pinned
    if ($UsePinned -and (Test-Path $PinnedPython)) {
        try { & $PinnedPython -c "import celery" 2>$null; if ($LASTEXITCODE -eq 0) { return $PinnedPython } } catch {}
    }
    # 2) venv locales
    $cands = @(
        Join-Path $ProjectDir "env\Scripts\python.exe",
        Join-Path $ProjectDir ".venv\Scripts\python.exe"
    )
    # 3) conda envs tipicos
    $roots = @("C:\Users\Promecor\miniconda3\envs","C:\Users\Admin\Miniconda3\envs")
    foreach ($r in $roots) {
        if (Test-Path $r) {
            Get-ChildItem -Path $r -Directory | ForEach-Object {
                $cands += (Join-Path $_.FullName "python.exe")
            }
        }
    }
    # 4) PATH
    $inPath = (& where.exe python) 2>$null
    $cands += $inPath

    foreach ($py in $cands | Select-Object -Unique) {
        if (-not [string]::IsNullOrWhiteSpace($py) -and (Test-Path $py)) {
            try { & $py -c "import celery" 2>$null; if ($LASTEXITCODE -eq 0) { return $py } } catch {}
        }
    }
    return $null
}

function Start-Tunnel {
    if (-not (Test-Path $TunnelBat)) { [System.Windows.MessageBox]::Show("No se encontro el script de tunel:`n$TunnelBat","Error","OK","Error"); return }
    $cloud = Get-Process -Name cloudflared -ErrorAction SilentlyContinue
    if (-not $cloud) { Start-Process -FilePath $TunnelBat | Out-Null; Start-Sleep -Seconds 2 }
    Start-Process $AppUrl | Out-Null
}

function Stop-Tunnel {
    Get-Process -Name cloudflared -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}

function Start-Celery {
    param([ValidateSet('florencia','productores')][string]$Alias,[string]$PythonExe)
    Stop-Celery | Out-Null
    $cmd = @"
`$host.ui.RawUI.WindowTitle = 'Celery - $Alias';
Set-Location '$ProjectDir';
`$env:GMAIL_SENDER_ALIAS = '$Alias';
& '$PythonExe' -m celery -A cobranzas_project worker -l info --pool=solo
"@
    Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit","-NoLogo","-Command",$cmd | Out-Null
    Start-Process $AppUrl | Out-Null
}

function Stop-Celery {
    Get-Process | Where-Object { $_.MainWindowTitle -in @('Celery - florencia','Celery - productores') } | Stop-Process -Force -ErrorAction SilentlyContinue
}

function Get-Status {
    $o = [ordered]@{}
    $o.Tunnel   = [bool](Get-Process -Name cloudflared -ErrorAction SilentlyContinue)
    $o.CelFlor  = [bool](Get-Process | ? { $_.MainWindowTitle -eq 'Celery - florencia' })
    $o.CelProd  = [bool](Get-Process | ? { $_.MainWindowTitle -eq 'Celery - productores' })
    $o.Python   = if ($global:PythonExe) { $global:PythonExe } else { "(no detectado)" }
    [pscustomobject]$o
}

function Show-Diag {
    $py = $global:PythonExe
    if (-not $py) { $py = Get-PythonWithCelery }
    $pyver = "" ; $celver = ""
    if ($py) {
        try { $pyver = (& $py -V) -join "`n" } catch {}
        try { $celver = (& $py -m celery --version) -join "`n" } catch {}
    }
    $txt = "Proyecto : $ProjectDir`nTunnelBat: $TunnelBat`nURL      : $AppUrl`n`nPython  : $py`n$pyver`n$celver"
    [System.Windows.MessageBox]::Show($txt,"Diagnostico") | Out-Null
}
#endregion

#region WPF UI
Add-Type -AssemblyName PresentationCore,PresentationFramework

$Xaml = @"
<Window xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
        Title="Promecor - Cobranzas" Height="380" Width="640"
        WindowStartupLocation="CenterScreen" ResizeMode="NoResize" Background="#111827">
  <Grid Margin="16">
    <Grid.RowDefinitions>
      <RowDefinition Height="Auto"/>
      <RowDefinition Height="*"/>
      <RowDefinition Height="Auto"/>
    </Grid.RowDefinitions>
    <Grid.ColumnDefinitions>
      <ColumnDefinition Width="2*"/>
      <ColumnDefinition Width="3*"/>
    </Grid.ColumnDefinitions>

    <TextBlock Grid.Row="0" Grid.ColumnSpan="2" Text="Promecor  Cobranzas" Foreground="White" FontSize="22" FontWeight="SemiBold" Margin="0,0,0,12"/>

    <StackPanel Grid.Row="1" Grid.Column="0" Margin="0,0,12,0">
      <Button x:Name="BtnStartTunnel"   Content="Start tunnel"   Height="36" Margin="0,0,0,8" />
      <Button x:Name="BtnStopTunnel"    Content="Stop tunnel"    Height="36" Margin="0,0,0,8" />
      <Separator Margin="0,4"/>
      <Button x:Name="BtnSendFlor"      Content="Send ART - Promecor" Height="36" Margin="0,8,0,8" />
      <Button x:Name="BtnSendProd"      Content="Send ART - Productores" Height="36" Margin="0,0,0,8" />
      <Button x:Name="BtnStopCelery"    Content="Stop Celery"    Height="36" Margin="0,0,0,8" />
      <Separator Margin="0,4"/>
      <Button x:Name="BtnOpenWeb"       Content="Open web"       Height="36" Margin="0,4,0,8" />
      <Button x:Name="BtnDiag"          Content="Diagnostics"    Height="36" />
    </StackPanel>

    <Border Grid.Row="1" Grid.Column="1" CornerRadius="8" Background="#0B1220" Padding="16">
      <StackPanel>
        <TextBlock Text="Status" Foreground="#93C5FD" FontSize="16" FontWeight="SemiBold" Margin="0,0,0,12"/>
        <StackPanel Orientation="Horizontal" Margin="0,0,0,6">
          <TextBlock Text="Tunnel:" Width="120" Foreground="#D1D5DB"/>
          <TextBlock x:Name="LblTunnel" Text="-" Foreground="White"/>
        </StackPanel>
        <StackPanel Orientation="Horizontal" Margin="0,0,0,6">
          <TextBlock Text="Celery Florencia:" Width="120" Foreground="#D1D5DB"/>
          <TextBlock x:Name="LblFlor" Text="-" Foreground="White"/>
        </StackPanel>
        <StackPanel Orientation="Horizontal" Margin="0,0,0,6">
          <TextBlock Text="Celery Productores:" Width="120" Foreground="#D1D5DB"/>
          <TextBlock x:Name="LblProd" Text="-" Foreground="White"/>
        </StackPanel>
        <StackPanel Orientation="Horizontal" Margin="0,0,0,6">
          <TextBlock Text="Python:" Width="120" Foreground="#D1D5DB"/>
          <TextBlock x:Name="LblPython" Text="-" Foreground="#A7F3D0"/>
        </StackPanel>
        <Button x:Name="BtnRefresh" Content="Refresh status" Height="32" Width="140" HorizontalAlignment="Left" Margin="0,8,0,0"/>
      </StackPanel>
    </Border>

    <TextBlock Grid.Row="2" Grid.ColumnSpan="2" Text="Tip: deja abiertas las ventanas de Celery durante el envio." Foreground="#9CA3AF" FontSize="12" Margin="0,12,0,0"/>
  </Grid>
</Window>
"@

[xml]$xml = $Xaml
$reader = New-Object System.Xml.XmlNodeReader $xml
$Window = [Windows.Markup.XamlReader]::Load($reader)

# Grab controls
$BtnStartTunnel = $Window.FindName("BtnStartTunnel")
$BtnStopTunnel  = $Window.FindName("BtnStopTunnel")
$BtnSendFlor    = $Window.FindName("BtnSendFlor")
$BtnSendProd    = $Window.FindName("BtnSendProd")
$BtnStopCelery  = $Window.FindName("BtnStopCelery")
$BtnOpenWeb     = $Window.FindName("BtnOpenWeb")
$BtnDiag        = $Window.FindName("BtnDiag")
$BtnRefresh     = $Window.FindName("BtnRefresh")

$LblTunnel      = $Window.FindName("LblTunnel")
$LblFlor        = $Window.FindName("LblFlor")
$LblProd        = $Window.FindName("LblProd")
$LblPython      = $Window.FindName("LblPython")

# Select python on load
$global:PythonExe = if (Test-Path $PinnedPython) { $PinnedPython } else { Get-PythonWithCelery }

function Update-StatusLabels {
    $s = Get-Status
    $LblTunnel.Text = if ($s.Tunnel) { "ACTIVE" } else { "NO" }
    $LblFlor.Text   = if ($s.CelFlor) { "active" } else { "no" }
    $LblProd.Text   = if ($s.CelProd) { "active" } else { "no" }
    $LblPython.Text = $s.Python
}

# Wire events
$BtnStartTunnel.Add_Click({ Start-Tunnel; Update-StatusLabels })
$BtnStopTunnel.Add_Click({ Stop-Tunnel; Update-StatusLabels })
$BtnSendFlor.Add_Click({
    if (-not $global:PythonExe) { $global:PythonExe = Get-PythonWithCelery }
    if ($global:PythonExe) { Start-Celery -Alias 'florencia' -PythonExe $global:PythonExe }
    else { [System.Windows.MessageBox]::Show("No hay Python con Celery.","Aviso") | Out-Null }
    Update-StatusLabels
})
$BtnSendProd.Add_Click({
    if (-not $global:PythonExe) { $global:PythonExe = Get-PythonWithCelery }
    if ($global:PythonExe) { Start-Celery -Alias 'productores' -PythonExe $global:PythonExe }
    else { [System.Windows.MessageBox]::Show("No hay Python con Celery.","Aviso") | Out-Null }
    Update-StatusLabels
})
$BtnStopCelery.Add_Click({ Stop-Celery; Update-StatusLabels })
$BtnOpenWeb.Add_Click({ Start-Process $AppUrl | Out-Null })
$BtnDiag.Add_Click({ Show-Diag })
$BtnRefresh.Add_Click({ Update-StatusLabels })

# Init status and show
Update-StatusLabels
$Window.ShowDialog() | Out-Null
#endregion
