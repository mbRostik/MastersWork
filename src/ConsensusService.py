import asyncio
import json
import logging
import math
import os
import ssl

import aio_pika
import httpx

from reputation_zkp import check_verifier, proof_envelope, verify_proof
from validator_quorum import MAX_PROOF_BYTES, collect_votes, load_validators, required_votes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("ConsensusService")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost//")
QUEUE_NAME = "consensus_queue"
DOTNET_BACKEND_URL = os.getenv("REPUTATION_URL", "https://localhost:7075/user/updatereputation")


class QuorumUnavailable(Exception):
    pass


async def process_zkp_message(payload, validators, backend, validator_client, channel, vote_timeout):
    if not await verify_proof(payload):
        logger.warning("Discarding invalid Groth16 proof")
        return

    validator_count = len(validators)
    quorum = required_votes(validator_count)
    positive_votes = await collect_votes(validator_client, validators, proof_envelope(payload), vote_timeout)
    approved = positive_votes >= quorum
    commitment = payload["commitment"]
    if approved:
        response = await backend.post(DOTNET_BACKEND_URL, json={"commitment": commitment})
        if response.status_code == 404:
            status = "POST_NOT_FOUND"
        else:
            response.raise_for_status()
            status = "APPROVED"
    else:
        status = "NO_QUORUM"

    outcome = {
        "action_type": "consensus",
        "commitment": commitment,
        "status": status,
        "positive_votes": positive_votes,
        "validator_count": validator_count,
        "quorum": quorum,
    }
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(outcome).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="analytics_queue",
        mandatory=True,
    )
    logger.info("Proof outcome %s with %d/%d votes (quorum %d)", status, positive_votes, validator_count, quorum)
    if not approved:
        raise QuorumUnavailable("Retrying valid proof after an unavailable quorum")


async def main():
    token = os.getenv("REPUTATION_SERVICE_TOKEN")
    if not token:
        raise RuntimeError("Set REPUTATION_SERVICE_TOKEN before starting ConsensusService")
    validators = load_validators(os.getenv("CONSENSUS_VALIDATOR_CONFIG", "[]"))
    vote_timeout = float(os.getenv("CONSENSUS_VOTE_TIMEOUT", "15"))
    if not math.isfinite(vote_timeout) or vote_timeout <= 0:
        raise ValueError("CONSENSUS_VOTE_TIMEOUT must be positive and finite")
    await check_verifier()
    tls = ssl.create_default_context(cafile=os.getenv("REPUTATION_CA_FILE") or None)
    validator_tls = ssl.create_default_context(cafile=os.getenv("CONSENSUS_CA_FILE") or None)

    async with (
        httpx.AsyncClient(verify=tls, headers={"Authorization": f"Bearer {token}"}) as backend,
        httpx.AsyncClient(verify=validator_tls, timeout=vote_timeout, trust_env=False) as validator_client,
    ):
        while True:
            try:
                connection = await aio_pika.connect_robust(RABBITMQ_URL)
                async with connection:
                    channel = await connection.channel(on_return_raises=True)
                    await channel.set_qos(prefetch_count=1)
                    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                    await channel.declare_queue("analytics_queue", durable=True)
                    logger.info("Listening for Groth16 proofs with %d configured validators", len(validators))
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process(requeue=True):
                                try:
                                    if len(message.body) > MAX_PROOF_BYTES:
                                        raise ValueError("Proof envelope too large")
                                    payload = json.loads(message.body)
                                except (ValueError, RecursionError):
                                    logger.warning("Discarding malformed proof envelope")
                                    continue
                                await process_zkp_message(payload, validators, backend, validator_client, channel, vote_timeout)
            except Exception as error:
                logger.error("Consensus delivery failed (%s). Retrying in 5 seconds", type(error).__name__)
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down ConsensusService")
