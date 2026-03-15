@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

set PYTHON_DIR=%~dp0python
set PYTHON_URL=https://github.com/astral-sh/python-build-standalone/releases/download/20260310/cpython-3.11.15+20260310-x86_64-pc-windows-msvc-install_only.tar.gz

echo ============================================
echo  KKS to SBV2 Pipeline - Setup
echo ============================================
echo.

if exist "%PYTHON_DIR%\python.exe" (
    echo [OK] Python already set up.
    echo.
    goto :install_deps
)

echo Downloading Python (includes tkinter) ...
echo.

set ARCHIVE=%TEMP%\kks_python.tar.gz
curl -L --progress-bar "%PYTHON_URL%" -o "%ARCHIVE%"
if !errorlevel! neq 0 (
    echo [ERROR] Download failed. Check your internet connection.
    pause
    exit /b 1
)

echo.
echo Extracting ...
set EXTRACT_DIR=%~dp0_python_extract
mkdir "%EXTRACT_DIR%" 2>nul
tar -xzf "%ARCHIVE%" -C "%EXTRACT_DIR%"
if !errorlevel! neq 0 (
    echo [ERROR] Extraction failed.
    del "%ARCHIVE%" 2>nul
    rmdir /s /q "%EXTRACT_DIR%" 2>nul
    pause
    exit /b 1
)

del "%ARCHIVE%" 2>nul

:: python-build-standalone extracts to "python/" inside the archive
if exist "%EXTRACT_DIR%\python\python.exe" (
    move "%EXTRACT_DIR%\python" "%PYTHON_DIR%"
) else (
    echo [ERROR] Unexpected archive structure.
    rmdir /s /q "%EXTRACT_DIR%" 2>nul
    pause
    exit /b 1
)

rmdir /s /q "%EXTRACT_DIR%" 2>nul

if not exist "%PYTHON_DIR%\python.exe" (
    echo [ERROR] Setup failed. Please try again.
    pause
    exit /b 1
)

echo.
echo [OK] Python setup complete!

:install_deps
echo Installing dependencies ...
echo.
"%PYTHON_DIR%\python.exe" -m pip install --upgrade pip --quiet
"%PYTHON_DIR%\python.exe" -m pip install UnityPy --quiet
if !errorlevel! neq 0 (
    echo [ERROR] pip install failed. Check your internet connection.
    pause
    exit /b 1
)
echo [OK] Dependencies installed.

:done
echo Run run_kks_all_in_one_gui.bat to start.
echo.
pause
