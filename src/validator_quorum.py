import asyncio
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx


MAX_PROOF_BYTES = 16_384
CHALLENGE_HEADER = "X-Reputation-Challenge"
AUTH_HEADER = "X-Reputation-Auth"
VOTE_HEADER = "X-Reputation-Vote"


def load_key(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("Each validator requires an independent 32-byte hex key")
    return bytes.fromhex(value)


def is_challenge(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value) is not None


def authenticate(key, challenge, proof, vote=None):
    # Separate request and response MAC domains. The response is valid only
    # for this proof, this challenge and this validator's key.
    domain = b"reputation-request-v1" if vote is None else b"reputation-vote-v1/" + vote.encode("ascii")
    return hmac.digest(key, domain + bytes.fromhex(challenge) + hashlib.sha256(proof).digest(), "sha256").hex()


def matches(expected, supplied):
    return isinstance(supplied, str) and supplied.isascii() and hmac.compare_digest(expected, supplied)


@dataclass(frozen=True)
class Validator:
    url: str
    key: bytes = field(repr=False)


def load_validators(config):
    try:
        entries = json.loads(config)
        if not isinstance(entries, list):
            raise ValueError
        validators = []
        for entry in entries:
            url = httpx.URL(entry["url"])
            parts = urlsplit(str(url))
            local = parts.hostname in {"localhost", "127.0.0.1", "::1"}
            if (not parts.hostname or parts.username is not None or parts.fragment or parts.query
                    or (parts.scheme != "https" and not (parts.scheme == "http" and local))):
                raise ValueError
            validators.append(Validator(str(url), load_key(entry["key"])))
        if (len(validators) < 4 or len({node.url for node in validators}) != len(validators)
                or len({node.key for node in validators}) != len(validators)):
            raise ValueError
        return tuple(validators)
    except (ValueError, KeyError, TypeError, httpx.InvalidURL):
        raise ValueError("Configure at least four distinct validator URLs and keys in CONSENSUS_VALIDATOR_CONFIG") from None


def required_votes(validator_count):
    return (2 * validator_count) // 3 + 1


async def _request_vote(client, node, challenge, proof):
    headers = {
        CHALLENGE_HEADER: challenge,
        AUTH_HEADER: authenticate(node.key, challenge, proof),
        "Content-Type": "application/json",
    }
    try:
        async with client.stream("POST", node.url, content=proof, headers=headers) as response:
            vote = response.headers.get(VOTE_HEADER)
            if response.status_code == 204 and vote in ("yes", "no"):
                signature = authenticate(node.key, challenge, proof, vote)
                if matches(signature, response.headers.get(AUTH_HEADER)):
                    return vote == "yes"
    except httpx.RequestError:
        pass
    return False


async def collect_votes(client, validators, payload, timeout):
    proof = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    challenge = secrets.token_hex(16)
    quorum = required_votes(len(validators))
    pending = {asyncio.create_task(_request_vote(client, node, challenge, proof)) for node in validators}
    approvals = 0
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while pending and approvals < quorum:
            done, pending = await asyncio.wait(pending, timeout=max(0, deadline - asyncio.get_running_loop().time()),
                                               return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            approvals += sum(task.result() for task in done)
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    return approvals
