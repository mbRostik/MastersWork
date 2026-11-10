import asyncio
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path


_ROOT = Path(__file__).resolve().parent / "zkp"
_ARTIFACTS = Path(os.getenv("ZKP_ARTIFACT_DIR", str(_ROOT / "artifacts"))).resolve()
_SCALAR_FIELD = 21888242871839275222246405745257275088548364400416034343698204186575808495617
_BASE_FIELD = 21888242871839275222246405745257275088696311157297823662689037894645226208583
_DECIMAL = re.compile(r"0|[1-9][0-9]{0,76}")


def _field_element(value, modulus):
    return isinstance(value, str) and _DECIMAL.fullmatch(value) is not None and int(value) < modulus


def is_commitment(value):
    return _field_element(value, _SCALAR_FIELD)


def _point(values, size):
    return (isinstance(values, list) and len(values) == size
            and all(_field_element(value, _BASE_FIELD) for value in values))


def _valid_payload(payload):
    if not isinstance(payload, dict) or not is_commitment(payload.get("commitment")):
        return False
    signals = payload.get("public_signals")
    if (not isinstance(signals, list) or len(signals) != 2
            or not is_commitment(signals[0]) or signals[1] != payload["commitment"]):
        return False
    proof = payload.get("proof_data")
    if not isinstance(proof, dict) or proof.get("protocol") != "groth16" or proof.get("curve") != "bn128":
        return False
    a, b, c = proof.get("pi_a"), proof.get("pi_b"), proof.get("pi_c")
    return (_point(a, 3) and a[2] == "1" and _point(c, 3) and c[2] == "1"
            and isinstance(b, list) and len(b) == 3 and b[2] == ["1", "0"]
            and _point(b[0], 2) and _point(b[1], 2))


async def _snark(command, payload=None):
    process = await asyncio.to_thread(
        subprocess.run, ["node", str(_ROOT / "snark.mjs"), command, str(_ARTIFACTS)],
        input=json.dumps(payload).encode(), capture_output=True, timeout=60, check=False,
    )
    if process.returncode:
        raise RuntimeError(f"Groth16 {command} failed. Check the installed reputation circuit artifacts.")
    return json.loads(process.stdout)


async def check_prover():
    await _snark("check-prover")


async def check_verifier():
    await _snark("check-verifier")


def _digest(value):
    # Keep 248 bits so the digest fits in the scalar field without reduction.
    return int.from_bytes(hashlib.sha256(value.encode("utf-8")).digest()[:31], "big")


async def generate_proof(commitment, post, metrics):
    salt = bytes.fromhex(post["salt"])
    if len(salt) != 32:
        raise ValueError("Expected a 32-byte post salt")
    witness = {
        "postCommitment": commitment,
        "author": _digest(post["userId"]),
        "postId": post["postId"],
        "textDigest": _digest(post["text"]),
        "saltHigh": int.from_bytes(salt[:16], "big"),
        "saltLow": int.from_bytes(salt[16:], "big"),
        "sentiment": metrics["sentiment_score"],
        "toxicity": metrics["toxicity_score"],
        "quality": metrics["overall_quality"],
    }
    result = await _snark("prove", {name: str(value) for name, value in witness.items()})
    payload = {
        "commitment": commitment,
        "public_signals": result["publicSignals"],
        "proof_data": result["proof"],
    }
    if not _valid_payload(payload):
        raise RuntimeError("Prover returned unexpected public signals or proof format")
    return payload


async def verify_proof(payload):
    if not _valid_payload(payload):
        return False
    return await _snark("verify", proof_envelope(payload)) is True


def proof_envelope(payload):
    """Copy only the public fields of an already validated payload."""
    return {
        "commitment": payload["commitment"],
        "public_signals": payload["public_signals"],
        "proof_data": {name: payload["proof_data"][name] for name in ("pi_a", "pi_b", "pi_c", "protocol", "curve")},
    }
