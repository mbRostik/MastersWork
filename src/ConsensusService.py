import asyncio
import json
import logging
import math
import os
import ssl

import aio_pika
from aio_pika.exceptions import AMQPError
import httpx

from launch_trust_system import rabbitmq_connection_options
from reputation_zkp import check_verifier, proof_envelope, verify_proof
from validator_quorum import MAX_PROOF_BYTES, collect_votes, load_validators, required_votes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("ConsensusService")

QUEUE_NAME = "consensus_queue"
DOTNET_BACKEND_URL = os.getenv("REPUTATION_URL", "https://localhost:7075/user/updatereputation")


class QuorumUnavailable(Exception):
    pass


class UnexpectedReputationResponse(Exception):
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
            if response.status_code != 204:
                raise UnexpectedReputationResponse(f"Expected HTTP 204, received {response.status_code}")
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
        timeout=5,
    )
    logger.info("Proof outcome %s with %d/%d votes (quorum %d)", status, positive_votes, validator_count, quorum)
    if not approved:
        raise QuorumUnavailable("Retrying valid proof after an unavailable quorum")


async def main():
    token = os.getenv("REPUTATION_SERVICE_TOKEN")
    if not token:
        raise RuntimeError("Set REPUTATION_SERVICE_TOKEN before starting ConsensusService")
    backend_url = httpx.URL(DOTNET_BACKEND_URL)
    if backend_url.scheme != "https" or not backend_url.host:
        raise ValueError("REPUTATION_URL must be an absolute HTTPS URL")
    connection_options = rabbitmq_connection_options(os.environ)
    validators = load_validators(os.getenv("CONSENSUS_VALIDATOR_CONFIG", "[]"))
    vote_timeout = float(os.getenv("CONSENSUS_VOTE_TIMEOUT", "15"))
    if not math.isfinite(vote_timeout) or vote_timeout <= 0:
        raise ValueError("CONSENSUS_VOTE_TIMEOUT must be positive and finite")
    await check_verifier()
    tls = ssl.create_default_context(cafile=os.getenv("REPUTATION_CA_FILE") or None)
    validator_tls = ssl.create_default_context(cafile=os.getenv("CONSENSUS_CA_FILE") or None)

    async with (
        httpx.AsyncClient(verify=tls, timeout=10, headers={"Authorization": f"Bearer {token}"}) as backend,
        httpx.AsyncClient(verify=validator_tls, timeout=vote_timeout, trust_env=False) as validator_client,
    ):
        while True:
            try:
                connection = await aio_pika.connect_robust(**connection_options, timeout=5)
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
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                if status < 500 and status not in (408, 429):
                    raise
                logger.error("Reputation API returned HTTP %d. Retrying in 5 seconds", status)
            except httpx.RequestError as error:
                logger.error("Reputation API request failed (%s). Retrying in 5 seconds", type(error).__name__)
            except QuorumUnavailable as error:
                logger.warning("%s in 5 seconds", error)
            except UnexpectedReputationResponse as error:
                logger.error("Reputation API response failed (%s). Retrying in 5 seconds", error)
            except (AMQPError, ConnectionError, TimeoutError) as error:
                logger.error("Consensus delivery failed (%s). Retrying in 5 seconds", type(error).__name__)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down ConsensusService")
