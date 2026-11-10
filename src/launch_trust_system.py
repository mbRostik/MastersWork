import argparse
import json
import math
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
STRING_SETTINGS = {
    "POST_WITNESS_TOKEN", "REPUTATION_SERVICE_TOKEN", "POST_SERVICE_URL", "REPUTATION_URL",
    "REPUTATION_CA_FILE", "RABBITMQ_URL", "CLICKHOUSE_HOST", "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB", "NLP_MODELS_PATH", "ZKP_ARTIFACT_DIR", "CONSENSUS_CA_FILE",
}
SETTINGS = STRING_SETTINGS | {
    "CLICKHOUSE_PORT", "TRUST_SKIP_DOCKER", "CONSENSUS_VALIDATOR_CONFIG", "CONSENSUS_VOTE_TIMEOUT",
}


def load_environment(path):
    environment = os.environ.copy()
    try:
        settings = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return environment
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid JSON at line {error.lineno}, column {error.colno}: {error.msg}") from None

    if not isinstance(settings, dict):
        raise ValueError("Expected an object with setting names and values")

    for name, value in settings.items():
        if name not in SETTINGS:
            raise ValueError(f"Unknown setting {name!r}")
        if value is None or environment.get(name):
            continue
        if name in STRING_SETTINGS:
            if not isinstance(value, str) or any(char in value for char in "\x00\r\n"):
                raise ValueError(f"{name} must be a single-line string")
            if name in {"NLP_MODELS_PATH", "ZKP_ARTIFACT_DIR"} and '"' in value:
                raise ValueError(f"{name} must be a path without quotation marks")
            environment[name] = value
        elif name == "CLICKHOUSE_PORT":
            if type(value) is not int or not 1 <= value <= 65535:
                raise ValueError(f"{name} must be an integer from 1 to 65535")
            environment[name] = str(value)
        elif name == "TRUST_SKIP_DOCKER":
            if type(value) is not bool:
                raise ValueError(f"{name} must be true or false")
            environment[name] = "1" if value else "0"
        elif name == "CONSENSUS_VOTE_TIMEOUT":
            if type(value) not in (int, float) or value <= 0 or (type(value) is float and not math.isfinite(value)):
                raise ValueError(f"{name} must be a positive finite number of seconds")
            environment[name] = str(value)
        elif name == "CONSENSUS_VALIDATOR_CONFIG":
            if not isinstance(value, list):
                raise ValueError(f"{name} must be an array of validator objects or null")
            environment[name] = json.dumps(value, separators=(",", ":"))
    return environment


def main():
    parser = argparse.ArgumentParser(prog="launch.bat")
    parser.add_argument("--check", action="store_true", help="validate the startup configuration")
    args = parser.parse_args()

    path = ROOT / "trust-system.json"
    try:
        environment = load_environment(path)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{path.name}: {error}\n")

    command = ["cmd.exe", "/d", "/c", "launch.bat", "--configured"]
    if args.check:
        command.append("--check")
    return subprocess.call(command, cwd=ROOT, env=environment)


if __name__ == "__main__":
    raise SystemExit(main())
