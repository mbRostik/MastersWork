import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import aio_pika
from clickhouse_driver import Client

from reputation_zkp import is_commitment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AnalyticsService")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost//")
QUEUE_NAME = "analytics_queue"
SCHEMA = Path(__file__).with_name("clickHouseTables.txt")
ch_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
    user=os.getenv("CLICKHOUSE_USER", "grafana_user"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "securepass"),
    database=os.getenv("CLICKHOUSE_DB", "analytics_db"),
)


def initialize_schema():
    for statement in SCHEMA.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            ch_client.execute(statement)


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


async def process_analytics_message(message):
    try:
        payload = json.loads(message.body)
        query, row = _analytics_insert(payload)
    except (KeyError, ValueError, TypeError, OverflowError, RecursionError):
        logger.warning("Discarding malformed analytics payload")
        return

    settings = {"insert_deduplication_token": message.message_id} if message.message_id else None
    await asyncio.to_thread(ch_client.execute, query, [row], settings=settings)
    logger.info("Saved %s event to ClickHouse", payload.get("action_type", "post"))


async def main():
    while True:
        try:
            await asyncio.to_thread(initialize_schema)
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                async with queue.iterator() as queue_iter:
                    logger.info("Listening for analytics and consensus outcomes")
                    async for message in queue_iter:
                        async with message.process(requeue=True):
                            await process_analytics_message(message)
        except Exception as error:
            logger.error("Analytics delivery failed (%s). Retrying in 5 seconds", type(error).__name__)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down AnalyticsService")
