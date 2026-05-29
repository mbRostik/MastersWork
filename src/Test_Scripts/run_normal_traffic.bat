@echo off
title Python Test Traffic Generator

echo =======================================================
echo  Starting Normal Traffic Simulation (50 posts / 5 min)
echo =======================================================
echo.

:: Прапорець /K залишить вікно відкритим, щоб ви бачили напис "test completed!"
start "Normal Traffic Generator" cmd /K python test_normal_traffic.py

echo Traffic generator is running in a separate window.