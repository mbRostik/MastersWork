import json
import os
import secrets
import subprocess
import sys
from pathlib import Path


def main():
    if not os.getenv("REPUTATION_SERVICE_TOKEN"):
        raise SystemExit("Set REPUTATION_SERVICE_TOKEN before starting the local cluster")

    directory = Path(__file__).resolve().parent
    coordinator_env = os.environ.copy()
    processes = []
    try:
        if not coordinator_env.get("CONSENSUS_VALIDATOR_CONFIG"):
            validator_env = coordinator_env.copy()
            for secret in (
                "REPUTATION_SERVICE_TOKEN", "POST_WITNESS_TOKEN", "CONSENSUS_VALIDATOR_CONFIG",
                "RABBITMQ_URL", "RABBITMQ_PASSWORD", "CLICKHOUSE_PASSWORD", "GF_SECURITY_ADMIN_PASSWORD",
            ):
                validator_env.pop(secret, None)

            validators = []
            for port in range(8031, 8041):
                key = secrets.token_hex(32)
                validators.append({"url": f"http://127.0.0.1:{port}/verify", "key": key})
                processes.append(subprocess.Popen(
                    [sys.executable, "-m", "uvicorn", "ValidatorService:app", "--host", "127.0.0.1",
                     "--port", str(port), "--no-access-log"],
                    cwd=directory, env={**validator_env, "REPUTATION_VALIDATOR_KEY": key},
                ))
            coordinator_env["CONSENSUS_VALIDATOR_CONFIG"] = json.dumps(validators)

        coordinator = subprocess.Popen(
            [sys.executable, "ConsensusService.py"], cwd=directory,
            env=coordinator_env,
        )
        processes.append(coordinator)
        return coordinator.wait()
    except KeyboardInterrupt:
        return 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
