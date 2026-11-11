@echo off
cd /d "%~dp0.." || exit /b 1
title Python Test Traffic Generator

echo =======================================================
echo  Starting Normal Traffic Simulation (50 posts / 5 min)
echo =======================================================
echo.

:: Прапорець /K залишить вікно відкритим, щоб ви бачили напис "test completed!"
start "Normal Traffic Generator" cmd /K python -m Test_Scripts.test_normal_traffic

echo Traffic generator is running in a separate window.
