# Create Python version startup shortcut
# Run with administrator privileges

$WshShell = New-Object -ComObject WScript.Shell
$pyDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Create main.py shortcut (starts all modules)
$Shortcut = $WshShell.CreateShortcut("$pyDir\main.lnk")
$Shortcut.TargetPath = "pythonw.exe"
$Shortcut.Arguments = "`"$pyDir\main.py`" --service"
$Shortcut.WorkingDirectory = $pyDir
$Shortcut.Description = "NetLimiter Monitor (Python)"
$Shortcut.Save()

# Set administrator flag
$bytes = [System.IO.File]::ReadAllBytes("$pyDir\main.lnk")
$bytes[21] = $bytes[21] -bor 32  # RunAsAdministrator flag
[System.IO.File]::WriteAllBytes("$pyDir\main.lnk", $bytes)

Write-Host "Shortcut created: $pyDir\main.lnk"
Write-Host "Double-click to run with admin privileges"
