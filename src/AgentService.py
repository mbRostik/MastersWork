import asyncio
import json
import logging
import os
import ssl
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import TypedDict

import aio_pika
import httpx
import torch
from fastapi import FastAPI
from langgraph.graph import END, START, StateGraph
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

from reputation_zkp import check_prover, generate_proof, is_commitment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TrustSystem.Agents")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost//")
EXCHANGE_NAME = "Posts.Application.UseCases.Handlers.Creation:IPostCreated_PublishEvent_From_PostWebApi"
QUEUE_NAME = "python_post_subscriber_queue"
POST_SERVICE_URL = os.getenv("POST_SERVICE_URL", "https://localhost:7028").rstrip("/")
BASE_PATH = Path(os.getenv("NLP_MODELS_PATH", str(Path(__file__).resolve().parent / "models")))

METRIC_SCALE = 10_000
COMPOSITE_SCALE = 100 * METRIC_SCALE
WEIGHTS = (30, 40, 30)  # Sentiment, non-toxicity, quality.
SCORE_THRESHOLD = 700_000
TOXICITY_LIMIT = 5_000


class TextAnalyzer:
    def __init__(self):
        device = 0 if torch.cuda.is_available() else -1
        self.sentiment_pipe = self._load("sentiment", device)
        self.toxicity_pipe = self._load("toxicity", device)
        self.quality_pipe = self._load("quality", device)

    @staticmethod
    def _load(name, device):
        directory = BASE_PATH / name
        return pipeline(
            "text-classification",
            model=AutoModelForSequenceClassification.from_pretrained(directory, local_files_only=True),
            tokenizer=AutoTokenizer.from_pretrained(directory, local_files_only=True),
            device=device,
        )

    def analyze(self, text):
        if not text.strip():
            return {"sentiment_score": 0.5, "toxicity_score": 0.0, "overall_quality": 0.0}

        sentiment = self.sentiment_pipe(text, truncation=True)[0]
        label = sentiment["label"].lower()
        sentiment_score = {"positive": sentiment["score"], "negative": 1 - sentiment["score"]}.get(label, 0.5)
        toxicity = self.toxicity_pipe(text, top_k=None, truncation=True, function_to_apply="sigmoid")
        quality = self.quality_pipe(text, top_k=None, truncation=True, function_to_apply="softmax")
        return {
            "sentiment_score": sentiment_score,
            "toxicity_score": next(item["score"] for item in toxicity if item["label"].lower() == "toxic"),
            "overall_quality": next(item["score"] for item in quality if item["label"] == "LABEL_1"),
        }


class AgentState(TypedDict, total=False):
    post_text: str
    commitment: str
    post_witness: dict | None
    analyzer: TextAnalyzer
    nlp_results: dict
    metric_units: dict
    final_score: float
    is_approved: bool
    zkp_payload: dict
    timestamp: str
    event_id: str | None
    channel: aio_pika.abc.AbstractChannel


def nlp_analyst_node(state: AgentState) -> dict:
    return {"nlp_results": state["analyzer"].analyze(state["post_text"])}


def scorer_node(state: AgentState) -> dict:
    units = {}
    for name, value in state["nlp_results"].items():
        probability = Decimal(str(value))
        if not probability.is_finite() or not 0 <= probability <= 1:
            raise ValueError(f"Invalid probability for {name}")
        units[name] = int((probability * METRIC_SCALE).to_integral_value(rounding=ROUND_HALF_UP))

    toxicity = units["toxicity_score"]
    components = (units["sentiment_score"], METRIC_SCALE - toxicity, units["overall_quality"])
    composite = sum(weight * component for weight, component in zip(WEIGHTS, components))
    return {
        "metric_units": units,
        "final_score": composite / COMPOSITE_SCALE,
        "is_approved": bool(state["post_text"].strip()) and toxicity <= TOXICITY_LIMIT and composite > SCORE_THRESHOLD,
    }


async def zkp_generator_node(state: AgentState) -> dict:
    return {"zkp_payload": await generate_proof(state["commitment"], state["post_witness"], state["metric_units"])}


async def publish_json(channel, queue, payload, message_id=None):
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                         message_id=message_id),
        routing_key=queue,
        mandatory=True,
    )


async def publisher_node(state: AgentState) -> dict:
    proof = state.get("zkp_payload")
    analytics_payload = {
        "timestamp": state["timestamp"],
        "action_type": "post",
        "commitment": state["commitment"],
        **{name: value / METRIC_SCALE for name, value in state["metric_units"].items()},
        "final_score": state["final_score"],
        "scoring_version": "composite-v1",
        "proof_generated": proof is not None,
    }
    await publish_json(state["channel"], "analytics_queue", analytics_payload, state.get("event_id"))
    if proof is not None:
        await publish_json(state["channel"], "consensus_queue", proof)
    return {}


def approval_router(state: AgentState) -> str:
    return "zkp_generator" if state["is_approved"] and state["post_witness"] is not None else "publisher"


workflow = StateGraph(AgentState)
workflow.add_node("nlp_analyst", nlp_analyst_node)
workflow.add_node("scorer", scorer_node)
workflow.add_node("zkp_generator", zkp_generator_node)
workflow.add_node("publisher", publisher_node)
workflow.add_edge(START, "nlp_analyst")
workflow.add_edge("nlp_analyst", "scorer")
workflow.add_conditional_edges("scorer", approval_router, ["zkp_generator", "publisher"])
workflow.add_edge("zkp_generator", "publisher")
workflow.add_edge("publisher", END)
agent_graph = workflow.compile()


async def process_post(message, channel, client, analyzer):
    try:
        body = json.loads(message.body)
        post_data = body["message"]["data"]
        if not isinstance(post_data, dict):
            raise ValueError("Expected a post object")
    except (ValueError, TypeError, KeyError, RecursionError):
        logger.warning("Discarding malformed post event")
        return

    commitment = post_data.get("commitment")
    witness = None
    if post_data.get("commitmentVersion") == 1:
        if not is_commitment(commitment):
            logger.warning("Discarding post event with an invalid ZKP commitment")
            return
        response = await client.get(f"{POST_SERVICE_URL}/post/reputation-witness/{commitment}")
        if response.status_code == 404:
            logger.info("Skipping post event because its post is no longer available")
            return
        response.raise_for_status()
        witness = response.json()
        text = witness["text"]
    else:
        # Pre-migration events still produce analytics but have no private opening.
        commitment = str(commitment or post_data.get("postId", ""))
        parts = (post_data.get("title"), post_data.get("content"))
        text = ". ".join(part for part in parts if isinstance(part, str) and part.strip())

    result = await agent_graph.ainvoke({
        "post_text": text,
        "commitment": commitment,
        "post_witness": witness,
        "analyzer": analyzer,
        "timestamp": body.get("sentTime") or datetime.now(timezone.utc).isoformat(),
        "event_id": message.message_id or (f"post:{commitment}" if witness is not None else None),
        "channel": channel,
    })
    logger.info(
        "Processed %s post event: score=%.5f proof_generated=%s",
        "analytics-only" if witness is None else "persisted",
        result["final_score"],
        result.get("zkp_payload") is not None,
    )


async def consume_rabbitmq_messages(analyzer, token):
    tls = ssl.create_default_context(cafile=os.getenv("REPUTATION_CA_FILE") or None)
    async with httpx.AsyncClient(verify=tls, headers={"Authorization": f"Bearer {token}"}) as client:
        while True:
            try:
                connection = await aio_pika.connect_robust(RABBITMQ_URL)
                async with connection:
                    channel = await connection.channel(on_return_raises=True)
                    await channel.set_qos(prefetch_count=1)
                    exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
                    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                    await queue.bind(exchange)
                    await channel.declare_queue("consensus_queue", durable=True)
                    await channel.declare_queue("analytics_queue", durable=True)
                    async with queue.iterator() as queue_iter:
                        logger.info("Listening for post events")
                        async for message in queue_iter:
                            async with message.process(requeue=True):
                                await process_post(message, channel, client, analyzer)
            except Exception as error:
                logger.error("Agent could not process messages (%s). Retrying in 5 seconds", type(error).__name__)
                await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    token = os.getenv("POST_WITNESS_TOKEN")
    if not token:
        raise RuntimeError("Set POST_WITNESS_TOKEN before starting AgentService")
    await check_prover()
    analyzer = await asyncio.to_thread(TextAnalyzer)
    task = asyncio.create_task(consume_rabbitmq_messages(analyzer, token))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"status": "LangGraph AI Agent System is running and listening to RabbitMQ"}
