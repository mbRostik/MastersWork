@echo off
cd /d "%~dp0.." || exit /b 1
title Python Bot Attack Simulator

echo =======================================================
echo  Initiating Automated Bot Attack Simulation (200 posts)
echo =======================================================
echo.

:: Прапорець /K покаже, скільки пакетів спаму було успішно виплюнуто в чергу
start "Bot Attack Simulator" cmd /K python -m Test_Scripts.test_bot_attack

echo Bot simulator is running in a separate window.
