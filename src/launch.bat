@echo off
setlocal EnableExtensions DisableDelayedExpansion
pushd "%~dp0" || exit /b 1

if /i "%~1"=="--configured" goto :configured
python launch_trust_system.py %*
set "_trust_exit=%errorlevel%"
popd
exit /b %_trust_exit%

:configured
shift
set "_trust_check="
if /i "%~1"=="--check" set "_trust_check=1"
if not "%~1"=="" if not defined _trust_check (
    echo Usage: launch.bat [--check] >&2
    goto :failed
)

if not defined POST_WITNESS_TOKEN (
    echo Set POST_WITNESS_TOKEN in trust-system.json to the same value configured in Posts. >&2
    goto :failed
)
if not defined REPUTATION_SERVICE_TOKEN (
    echo Set REPUTATION_SERVICE_TOKEN in trust-system.json to the same value configured in Posts and Users. >&2
    goto :failed
)

python -c "import aio_pika, clickhouse_driver, fastapi, httpx, langgraph.graph, torch, transformers, uvicorn" >nul
if errorlevel 1 (
    echo Use the Python environment with the Trust System dependencies installed. >&2
    echo python -m pip install aio-pika clickhouse-driver fastapi httpx langgraph torch transformers uvicorn >&2
    goto :failed
)
if defined CONSENSUS_VALIDATOR_CONFIG (
    python -c "import os; from validator_quorum import load_validators; load_validators(os.environ['CONSENSUS_VALIDATOR_CONFIG'])" >nul 2>&1 || (
        echo CONSENSUS_VALIDATOR_CONFIG requires at least four distinct validator URLs and 64-character hex keys. >&2
        goto :failed
    )
)
node --version >nul 2>&1 || (
    echo Node.js must be available on PATH. >&2
    goto :failed
)

if not defined NLP_MODELS_PATH set "NLP_MODELS_PATH=%CD%\models"
for %%M in (sentiment toxicity quality) do (
    if not exist "%NLP_MODELS_PATH%\%%M\config.json" (
        echo Model %%M is missing from "%NLP_MODELS_PATH%". Set NLP_MODELS_PATH to the models directory. >&2
        goto :failed
    )
)

set "_trust_local_zkp="
if not defined ZKP_ARTIFACT_DIR (
    set "ZKP_ARTIFACT_DIR=%CD%\zkp\artifacts"
    set "_trust_local_zkp=1"
)

if not exist "zkp\node_modules\snarkjs\package.json" (
    if defined _trust_check (
        echo Node dependencies are missing. Run npm ci --prefix zkp. >&2
        goto :failed
    )
    call :install_node_dependencies || goto :failed
)

if exist "%ZKP_ARTIFACT_DIR%\reputation_js\reputation.wasm" if exist "%ZKP_ARTIFACT_DIR%\reputation.zkey" if exist "%ZKP_ARTIFACT_DIR%\verification_key.json" goto :verify_zkp
if not defined _trust_local_zkp goto :incomplete_zkp
if exist "%ZKP_ARTIFACT_DIR%\reputation_js\reputation.wasm" goto :incomplete_zkp
if exist "%ZKP_ARTIFACT_DIR%\reputation.zkey" goto :incomplete_zkp
if exist "%ZKP_ARTIFACT_DIR%\verification_key.json" goto :incomplete_zkp
if defined _trust_check (
    echo Local ZKP artifacts are missing. Run npm --prefix zkp run setup:dev. >&2
    goto :failed
)
if not exist "zkp\node_modules\circom2\package.json" (
    call :install_node_dependencies || goto :failed
)
if not exist "zkp\node_modules\circomlib\package.json" (
    call :install_node_dependencies || goto :failed
)
echo Building local development ZKP artifacts...
call npm.cmd --prefix zkp run setup:dev || goto :failed

:verify_zkp
node zkp\snark.mjs check-prover "%ZKP_ARTIFACT_DIR%" >nul || goto :incomplete_zkp
node zkp\snark.mjs check-verifier "%ZKP_ARTIFACT_DIR%" >nul || goto :incomplete_zkp

if "%TRUST_SKIP_DOCKER%"=="1" goto :ready
docker info --format "{{.ServerVersion}}" >nul 2>&1 || (
    echo Start Docker Desktop or set TRUST_SKIP_DOCKER=1 for an existing RabbitMQ and ClickHouse. >&2
    goto :failed
)
docker compose version >nul 2>&1 || goto :failed
if defined _trust_check goto :ready
echo Starting RabbitMQ and ClickHouse...
docker compose up -d --wait --wait-timeout 90 rabbitmq clickhouse || goto :failed

:ready
if defined _trust_check (
    echo Trust System preflight passed.
    popd
    exit /b 0
)

start "Trust System - Analytics" "%ComSpec%" /d /k python -u AnalyticService.py || goto :failed
start "Trust System - Consensus" "%ComSpec%" /d /k python -u start_consensus_cluster.py || goto :failed
start "Trust System - Agent" "%ComSpec%" /d /k python -m uvicorn AgentService:app --host 127.0.0.1 --port 8020 || goto :failed
echo Trust System windows opened. Wait for AgentService to log "Listening for post events" before creating a post.
popd
exit /b 0

:install_node_dependencies
where npm.cmd >nul 2>&1 || (
    echo npm must be available on PATH to prepare the ZKP dependencies. >&2
    exit /b 1
)
call npm.cmd --prefix zkp ci --include=dev
exit /b %errorlevel%

:incomplete_zkp
echo Use a complete ZKP artifact set in "%ZKP_ARTIFACT_DIR%". >&2
echo Required: reputation_js\reputation.wasm, reputation.zkey, verification_key.json. >&2

:failed
echo Trust System launch failed. >&2
popd
exit /b 1
