@echo off
title Python Microservices Runner

echo Starting AgentService (FastAPI + LangGraph)...
start "AgentService (Port 8020)" python -m uvicorn AgentService:app --port 8020 --reload

echo Starting ConsensusService (RabbitMQ listener)...
:: Прапорець /K залишить вікно відкритим після помилки
start "ConsensusService" cmd /K python ConsensusService.py

echo Starting AnalyticsService (ClickHouse Logger)...
start "AnalyticsService" cmd /K python AnalyticService.py