"""Compatibility import for the original agent experiment."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from AgentService import app

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8020)
