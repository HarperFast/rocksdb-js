@echo off
setlocal

set "ITERATIONS=%~1"
if "%ITERATIONS%"=="" set "ITERATIONS=50"

rem Prefer PowerShell 7+ (pwsh) if available, otherwise fall back to Windows PowerShell.
where pwsh >nul 2>nul
if %ERRORLEVEL%==0 (
  pwsh -NoProfile -ExecutionPolicy Bypass -File "%~dp0compare-bundles.ps1" %ITERATIONS%
  exit /b %ERRORLEVEL%
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0compare-bundles.ps1" %ITERATIONS%
exit /b %ERRORLEVEL%


