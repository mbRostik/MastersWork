import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import aio_pika
from aio_pika.exceptions import AMQPError
from clickhouse_driver import Client
from clickhouse_driver.errors import ErrorCodes, NetworkError, ServerException, SocketTimeoutError

from launch_trust_system import rabbitmq_connection_options, required_setting
from reputation_zkp import is_commitment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AnalyticsService")

QUEUE_NAME = "analytics_queue"
SCHEMA = Path(__file__).with_name("clickHouseTables.txt")
CLICKHOUSE_ERRORS = (NetworkError, SocketTimeoutError, ServerException, ConnectionError, TimeoutError)
CLICKHOUSE_RETRY_CODES = {
    ErrorCodes.NETWORK_ERROR,
    ErrorCodes.SOCKET_TIMEOUT,
    ErrorCodes.TIMEOUT_EXCEEDED,
    ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES,
}


def create_clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=required_setting(os.environ, "CLICKHOUSE_USER"),
        password=required_setting(os.environ, "CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DB", "analytics_db"),
        connect_timeout=5,
        send_receive_timeout=10,
        sync_request_timeout=5,
        settings={"max_execution_time": 10},
    )


class ClickHouseUnavailable(Exception):
    pass


async def run_clickhouse(operation, *args, **kwargs):
    # Socket timeouts terminate the driver's call before we reuse its connection.
    for delay in (0.5, 1, None):
        try:
            return await asyncio.to_thread(operation, *args, **kwargs)
        except CLICKHOUSE_ERRORS as error:
            if isinstance(error, ServerException) and error.code not in CLICKHOUSE_RETRY_CODES:
                raise
            if delay is None:
                raise ClickHouseUnavailable(f"ClickHouse failed after three attempts ({type(error).__name__})") from error
            logger.warning("ClickHouse operation failed (%s). Retrying in %s seconds",
                           type(error).__name__, delay)
            await asyncio.sleep(delay)


def initialize_schema(client):
    for statement in SCHEMA.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.execute(statement)


def _probability(value):
    value = float(value)
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("Invalid analytics metric")
    return value


def _analytics_insert(payload):
    commitment = payload["commitment"]
    if not isinstance(commitment, str) or len(commitment) > 128:
        raise ValueError("Invalid analytics commitment")

    if payload.get("action_type") == "consensus":
        status = payload["status"]
        votes, count, quorum = (payload[field] for field in ("positive_votes", "validator_count", "quorum"))
        if (not is_commitment(commitment) or status not in ("APPROVED", "POST_NOT_FOUND", "REJECTED", "NO_QUORUM")
                or not all(type(value) is int for value in (votes, count, quorum))
                or not 0 <= votes <= count < 2**32 or quorum != (2 * count) // 3 + 1):
            raise ValueError("Invalid consensus outcome")
        return """
            INSERT INTO post_consensus (commitment, status, positive_votes, validator_count, quorum) VALUES
        """, (commitment, status, votes, count, quorum)

    event_time = datetime.fromisoformat(payload["timestamp"])
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    score = payload.get("final_score")
    scoring_version = payload.get("scoring_version")
    if payload.get("action_type", "post") != "post" or scoring_version not in (None, "composite-v1"):
        raise ValueError("Unknown analytics event or scoring version")
    return """
        INSERT INTO post_analytics (
            event_time, event_type, content_quality, sentiment_score, toxicity_score,
            commitment, proof_generated, final_score, scoring_version
        ) VALUES
    """, (
        event_time,
        "post",
        _probability(payload.get("overall_quality", 0)),
        _probability(payload.get("sentiment_score", 0.5)),
        _probability(payload.get("toxicity_score", 0)),
        commitment,
        payload.get("proof_generated") is True,
        None if score is None else _probability(score),
        scoring_version,
    )


async def process_analytics_message(message, client):
    try:
        payload = json.loads(message.body)
        query, row = _analytics_insert(payload)
    except (KeyError, ValueError, TypeError, OverflowError, RecursionError):
        logger.warning("Discarding malformed analytics payload")
        return

    token = message.message_id or sha256(message.body).hexdigest()
    await run_clickhouse(client.execute, query, [row], settings={
        "async_insert": 0,
        "insert_deduplicate": 1,
        "insert_deduplication_token": token,
    })
    logger.info("Saved %s event to ClickHouse", payload.get("action_type", "post"))


async def main():
    connection_options = rabbitmq_connection_options(os.environ)
    client = create_clickhouse_client()
    initialized = False
    while True:
        try:
            if not initialized:
                await run_clickhouse(initialize_schema, client)
                initialized = True
            connection = await aio_pika.connect_robust(**connection_options, timeout=5)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                async with queue.iterator() as queue_iter:
                    logger.info("Listening for analytics and consensus outcomes")
                    async for message in queue_iter:
                        async with message.process(requeue=True):
                            await process_analytics_message(message, client)
        except ClickHouseUnavailable as error:
            logger.error("%s. Pending delivery will be retried in 5 seconds", error)
            await asyncio.sleep(5)
        except (AMQPError, ConnectionError, TimeoutError) as error:
            logger.error("Analytics broker unavailable (%s). Reconnecting in 5 seconds", type(error).__name__)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down AnalyticsService")
