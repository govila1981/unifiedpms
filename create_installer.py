"""
Alternative to .exe - Create a simple installer that sets up Python environment
No antivirus issues, easier distribution
"""

import os
import sys
from pathlib import Path

def create_installer_script():
    """Create a batch file installer that sets up the app without exe"""

    installer_content = """@echo off
echo ========================================================================
echo   Trade Processing Pipeline - Easy Installer
echo   Version 5.0
echo ========================================================================
echo.
echo This installer will:
echo   1. Check if Python is installed
echo   2. Install required packages
echo   3. Create desktop shortcut
echo   4. You're ready to use the app!
echo.
pause

REM Check Python
echo [1/4] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Python is not installed!
    echo.
    echo Please install Python 3.8 or later from:
    echo https://www.python.org/downloads/
    echo.
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)

python --version
echo Python found!

REM Install dependencies
echo.
echo [2/4] Installing required packages...
echo This may take 2-5 minutes...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo ERROR: Failed to install packages
    pause
    exit /b 1
)
echo Packages installed successfully!

REM Create desktop shortcut
echo.
echo [3/4] Creating desktop shortcut...

set SCRIPT_DIR=%~dp0
set SHORTCUT_PATH=%USERPROFILE%\\Desktop\\Trade Processing.lnk

REM Create VBS script to make shortcut
echo Set oWS = WScript.CreateObject("WScript.Shell") > CreateShortcut.vbs
echo sLinkFile = "%SHORTCUT_PATH%" >> CreateShortcut.vbs
echo Set oLink = oWS.CreateShortcut(sLinkFile) >> CreateShortcut.vbs
echo oLink.TargetPath = "cmd.exe" >> CreateShortcut.vbs
echo oLink.Arguments = "/c cd /d ""%SCRIPT_DIR%"" && run_app.bat" >> CreateShortcut.vbs
echo oLink.WorkingDirectory = "%SCRIPT_DIR%" >> CreateShortcut.vbs
echo oLink.Description = "Trade Processing Pipeline" >> CreateShortcut.vbs
echo oLink.Save >> CreateShortcut.vbs

cscript CreateShortcut.vbs //nologo
del CreateShortcut.vbs

echo Desktop shortcut created!

REM Create run script
echo.
echo [4/4] Creating launcher script...

echo @echo off > run_app.bat
echo cd /d "%%~dp0" >> run_app.bat
echo echo Starting Trade Processing Pipeline... >> run_app.bat
echo echo. >> run_app.bat
echo echo Opening browser... >> run_app.bat
echo echo. >> run_app.bat
echo start http://localhost:8501 >> run_app.bat
echo timeout /t 2 /nobreak ^>nul >> run_app.bat
echo streamlit run unified-streamlit-app.py >> run_app.bat

echo Launcher created!

echo.
echo ========================================================================
echo   INSTALLATION COMPLETE!
echo ========================================================================
echo.
echo You can now:
echo   1. Double-click "Trade Processing" shortcut on your desktop, OR
echo   2. Run "run_app.bat" in this folder
echo.
echo The application will open in your web browser.
echo.
echo To close the app: Close the command window or press Ctrl+C
echo.
pause
"""

    # Write installer
    with open('INSTALL.bat', 'w') as f:
        f.write(installer_content)

    print("[OK] Created INSTALL.bat")
    print()
    print("Created alternative installer (no .exe, no antivirus issues)")
    print()
    print("To distribute:")
    print("1. Zip entire project folder")
    print("2. Send to users")
    print("3. Users unzip and run INSTALL.bat")
    print("4. Desktop shortcut created automatically")
    print()

if __name__ == "__main__":
    create_installer_script()
