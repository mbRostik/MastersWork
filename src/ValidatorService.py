import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response

from reputation_zkp import check_verifier, verify_proof
from validator_quorum import AUTH_HEADER, CHALLENGE_HEADER, MAX_PROOF_BYTES, VOTE_HEADER, authenticate, is_challenge, load_key, matches


@asynccontextmanager
async def lifespan(app):
    app.state.key = load_key(os.environ.get("REPUTATION_VALIDATOR_KEY"))
    await check_verifier()
    app.state.verifier_slot = asyncio.Semaphore(1)
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/verify", status_code=204)
async def vote(request: Request):
    challenge = request.headers.get(CHALLENGE_HEADER)
    if not is_challenge(challenge):
        raise HTTPException(status_code=401)

    proof = bytearray()
    async for chunk in request.stream():
        if len(proof) + len(chunk) > MAX_PROOF_BYTES:
            raise HTTPException(status_code=413)
        proof.extend(chunk)

    key = request.app.state.key
    if not matches(authenticate(key, challenge, proof), request.headers.get(AUTH_HEADER)):
        raise HTTPException(status_code=401)
    try:
        payload = json.loads(proof)
    except (ValueError, RecursionError):
        raise HTTPException(status_code=400) from None

    async with request.app.state.verifier_slot:
        ballot = "yes" if await verify_proof(payload) else "no"
    return Response(status_code=204, headers={VOTE_HEADER: ballot, AUTH_HEADER: authenticate(key, challenge, proof, ballot)})
