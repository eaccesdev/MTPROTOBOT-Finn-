@echo off
:: ============================================================
:: Telegram Proxy Manager Bot — Windows Launcher
:: ============================================================
title Telegram Proxy Manager Bot

:loop
echo Starting bot...
python bot.py
echo.
echo Bot stopped. Restarting in 5 seconds... (Ctrl+C to exit)
timeout /t 5 /nobreak >NUL
goto loop
