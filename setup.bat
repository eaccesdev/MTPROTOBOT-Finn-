@echo off
:: ============================================================
:: Telegram Proxy Manager Bot — Windows 7 / 10 / 11 Setup
:: ============================================================
:: Requirements: Python 3.8 or higher
:: Get Python from https://www.python.org/downloads/

echo ==========================================
echo  Telegram Proxy Manager Bot — Setup
echo ==========================================
echo.

:: Check Python
python --version 2>NUL
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH.
    echo Download it from: https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)

:: Upgrade pip silently
python -m pip install --upgrade pip --quiet

:: Install dependencies
echo Installing dependencies...
python -m pip install -r requirements.txt

if errorlevel 1 (
    echo [ERROR] Dependency install failed. Check your internet connection.
    pause
    exit /b 1
)

:: Copy config example if config.json doesn't exist
if not exist config.json (
    copy config.example.json config.json
    echo.
    echo [!] config.json created from template.
)

echo.
echo ==========================================
echo  Setup complete!
echo ==========================================
echo.
echo Next steps:
echo   1. Open config.json in Notepad
echo   2. Replace the bot_token value with your token from @BotFather
echo   3. Set your Telegram user ID in admin_ids
echo   4. Save the file, then run:  run.bat
echo.
pause
