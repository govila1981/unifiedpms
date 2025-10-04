@echo off
echo ====================================================================
echo   Building Standalone Executable for Trade Processing Pipeline
echo ====================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found!
    echo Please install Python from https://www.python.org/
    pause
    exit /b 1
)

echo [1/3] Installing PyInstaller...
pip install pyinstaller

echo.
echo [2/3] Building executable...
echo This will take 5-10 minutes...
python build_exe.py

echo.
echo [3/3] Done!
echo.
echo Your executable is ready at: dist\TradeProcessingPipeline.exe
echo.
pause
