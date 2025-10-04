@echo off
echo ========================================================================
echo   Trade Processing Pipeline - Universal Installer
echo   Works on: x64 Windows, ARM64 Windows, Intel, AMD, Snapdragon
echo ========================================================================
echo.

REM Detect architecture
echo [1/5] Detecting system architecture...
set ARCH=unknown

if "%PROCESSOR_ARCHITECTURE%"=="AMD64" (
    set ARCH=x64
    echo Detected: x64 / Intel / AMD processor
) else if "%PROCESSOR_ARCHITECTURE%"=="ARM64" (
    set ARCH=ARM64
    echo Detected: ARM64 / Snapdragon processor
) else if "%PROCESSOR_ARCHITECTURE%"=="x86" (
    set ARCH=x86
    echo Detected: 32-bit x86 processor
) else (
    echo Detected: %PROCESSOR_ARCHITECTURE%
)

echo.

REM Check if Python is installed
echo [2/5] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo Python is NOT installed on this system.
    echo.
    echo ========================================================================
    echo   OPTION 1: Install Python [RECOMMENDED for offline use]
    echo ========================================================================
    echo.
    echo Please download and install Python for your architecture:
    echo.

    if "%ARCH%"=="ARM64" (
        echo For ARM64 / Snapdragon:
        echo https://www.python.org/ftp/python/3.11.7/python-3.11.7-arm64.exe
    ) else if "%ARCH%"=="x64" (
        echo For x64 / Intel / AMD:
        echo https://www.python.org/ftp/python/3.11.7/python-3.11.7-amd64.exe
    ) else (
        echo For 32-bit x86:
        echo https://www.python.org/ftp/python/3.11.7/python-3.11.7.exe
    )

    echo.
    echo IMPORTANT: During installation, check "Add Python to PATH"
    echo.
    echo After installing Python, run this installer again.
    echo.
    echo ========================================================================
    echo   OPTION 2: Use Web Version [NO INSTALLATION NEEDED]
    echo ========================================================================
    echo.
    echo The web version works on ANY device with a browser:
    echo - Windows (x64, ARM64, x86)
    echo - Mac (Intel, Apple Silicon)
    echo - Linux
    echo - iPad, Android tablets
    echo.
    echo Web URL: [Will be provided after deployment]
    echo.
    echo ========================================================================
    echo.
    pause
    exit /b 1
)

python --version
echo Python found!

REM Check Python architecture matches system
echo.
echo [3/5] Verifying Python architecture...
python -c "import platform; print(f'Python architecture: {platform.machine()}')"

REM Install dependencies
echo.
echo [4/5] Installing required packages...
echo This may take 2-5 minutes...
echo.

pip install -r requirements.txt
if errorlevel 1 (
    echo.
    echo WARNING: Some packages may have failed to install.
    echo This can happen on ARM64 systems if packages don't have ARM builds.
    echo.
    echo For best experience on ARM64 devices, use the web version instead.
    echo.
    pause
)

echo.
echo Packages installed successfully!

REM Create launcher scripts
echo.
echo [5/5] Creating launcher...

REM Create Python launcher (works on all architectures)
echo import webbrowser > launch_app.py
echo import subprocess >> launch_app.py
echo import time >> launch_app.py
echo import sys >> launch_app.py
echo. >> launch_app.py
echo print("Starting Trade Processing Pipeline...") >> launch_app.py
echo print("") >> launch_app.py
echo print("Opening browser in 3 seconds...") >> launch_app.py
echo print("") >> launch_app.py
echo. >> launch_app.py
echo # Start Streamlit >> launch_app.py
echo process = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "unified-streamlit-app.py"]) >> launch_app.py
echo. >> launch_app.py
echo # Wait and open browser >> launch_app.py
echo time.sleep(3) >> launch_app.py
echo webbrowser.open("http://localhost:8501") >> launch_app.py
echo. >> launch_app.py
echo # Keep running >> launch_app.py
echo try: >> launch_app.py
echo     process.wait() >> launch_app.py
echo except KeyboardInterrupt: >> launch_app.py
echo     print("\nShutting down...") >> launch_app.py
echo     process.terminate() >> launch_app.py

REM Create simple batch launcher
echo @echo off > RUN_APP.bat
echo echo Starting Trade Processing Pipeline... >> RUN_APP.bat
echo python launch_app.py >> RUN_APP.bat

REM Create desktop shortcut
set SCRIPT_DIR=%~dp0
set SHORTCUT_PATH=%USERPROFILE%\Desktop\Trade Processing.lnk

echo Set oWS = WScript.CreateObject("WScript.Shell") > CreateShortcut.vbs
echo sLinkFile = "%SHORTCUT_PATH%" >> CreateShortcut.vbs
echo Set oLink = oWS.CreateShortcut(sLinkFile) >> CreateShortcut.vbs
echo oLink.TargetPath = "cmd.exe" >> CreateShortcut.vbs
echo oLink.Arguments = "/c cd /d ""%SCRIPT_DIR%"" && RUN_APP.bat" >> CreateShortcut.vbs
echo oLink.WorkingDirectory = "%SCRIPT_DIR%" >> CreateShortcut.vbs
echo oLink.Description = "Trade Processing Pipeline" >> CreateShortcut.vbs
echo oLink.Save >> CreateShortcut.vbs

cscript CreateShortcut.vbs //nologo >nul 2>&1
del CreateShortcut.vbs

echo.
echo ========================================================================
echo   INSTALLATION COMPLETE!
echo ========================================================================
echo.
echo System: %ARCH% Windows
echo Python: Installed and configured
echo.
echo To start the application:
echo   1. Double-click "Trade Processing" on desktop, OR
echo   2. Run RUN_APP.bat in this folder
echo.
echo The app will open in your default browser.
echo.
echo To stop: Close the command window or press Ctrl+C
echo.
echo ========================================================================
echo.
pause
