import asyncio
import json
import logging
import os
import torch
import warnings
from contextlib import asynccontextmanager
from fastapi import FastAPI
import aio_pika
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from datetime import datetime
from typing import TypedDict, Optional, Dict, Any
from langgraph.graph import StateGraph, START, END

# Suppress minor huggingface warnings for cleaner output
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TrustSystem.Agents")

# --- CONFIGURATION ---
RABBITMQ_URL = "amqp://guest:guest@localhost//"
EXCHANGE_NAME = "Posts.Application.UseCases.Handlers.Creation:IPostCreated_PublishEvent_From_PostWebApi"
QUEUE_NAME = "python_post_subscriber_queue"

BASE_PATH = r"D:\OverComeItBunchOfProjects\Personal\master\development\trust_system\models"

# --- AI ANALYZER CLASS ---
class TextAnalyzer:
    def __init__(self):
        self.device = 0 if torch.cuda.is_available() else -1
        device_name = "GPU" if self.device == 0 else "CPU"
        logger.info(f"Loading AI weights locally on {device_name}...")

        sent_dir = os.path.join(BASE_PATH, "sentiment")
        self.sentiment_pipe = pipeline("sentiment-analysis", 
                                       model=AutoModelForSequenceClassification.from_pretrained(sent_dir), 
                                       tokenizer=AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest"), 
                                       device=self.device)

        tox_dir = os.path.join(BASE_PATH, "toxicity")
        self.toxicity_pipe = pipeline("text-classification", 
                                      model=AutoModelForSequenceClassification.from_pretrained(tox_dir), 
                                      tokenizer=AutoTokenizer.from_pretrained("unitary/toxic-bert"), 
                                      device=self.device)

        qual_dir = os.path.join(BASE_PATH, "quality")
        self.quality_pipe = pipeline("text-classification", 
                                     model=AutoModelForSequenceClassification.from_pretrained(qual_dir), 
                                     tokenizer=AutoTokenizer.from_pretrained("textattack/roberta-base-CoLA"), 
                                     device=self.device)
        
        logger.info("✅ All AI Models loaded successfully!")

    def analyze(self, text):
        if not isinstance(text, str) or not text.strip():
            return {"sentiment_score": 0.5, "toxicity_score": 0.0, "overall_quality": 0.0}

        sent_res = self.sentiment_pipe(text)[0]
        sentiment_score = sent_res['score'] if sent_res['label'].lower() == 'positive' else (1.0 - sent_res['score'] if sent_res['label'].lower() == 'negative' else 0.5)

        tox_res = self.toxicity_pipe(text)[0]
        toxicity_score = tox_res['score'] if tox_res['label'].lower() == 'toxic' else 1.0 - tox_res['score']

        qual_res = self.quality_pipe(text)[0]
        quality_score = qual_res['score'] if qual_res['label'] == 'LABEL_1' else 1.0 - qual_res['score']

        return {
            "sentiment_score": round(float(sentiment_score), 4),
            "toxicity_score": round(float(toxicity_score), 4),
            "overall_quality": round(float(quality_score), 4)
        }

analyzer = TextAnalyzer()


# Крок 1: Проєктування Стану (State)
class AgentState(TypedDict):
    post_text: str
    commitment: str
    nlp_results: Optional[Dict[str, float]]
    final_score: Optional[float]
    is_approved: bool
    zkp_payload: Optional[Dict[str, Any]]
    analytics_payload: Optional[Dict[str, Any]]
    channel: Any  # Передаємо канал RabbitMQ для Publisher-вузла

# Крок 2: Створення Вузлів (Nodes)

def nlp_analyst_node(state: AgentState) -> dict:
    """Вузол 1: Аналізує текст за допомогою ШІ"""
    text = state["post_text"]
    results = analyzer.analyze(text)
    
    logger.info("  [NLP Analyst] Finished text analysis.")
    logger.info(f"    Sentiment: {results['sentiment_score']} | Toxicity: {results['toxicity_score']} | Quality: {results['overall_quality']}")
    
    return {"nlp_results": results}

def scorer_node(state: AgentState) -> dict:
    """Вузол 2: Рахує фінальний бал і перевіряє поріг"""
    results = state["nlp_results"]
    toxicity = results['toxicity_score']
    
    if toxicity > 0.5:
        final_score = 0.0
        logger.info(" [Scorer] Post rejected due to high toxicity.")
    else:
        final_score = round((results['overall_quality'] + results['sentiment_score']) / 2.0, 4)

    is_approved = final_score > 0.7
    logger.info(f" [Scorer] Final Score: {final_score}. Approved for ZKP: {is_approved}")
    
    return {"final_score": final_score, "is_approved": is_approved}

def zkp_generator_node(state: AgentState) -> dict:
    """Вузол 3: Генерує анонімний доказ (працює ТІЛЬКИ якщо пост схвалено)"""
    logger.info(" [Cryptographer] Generating ZKP Mock...")
    zkp_payload = {
        "commitment": state["commitment"],
        "public_signal": True,
        "proof_data": {
            "pi_a": ["mock_value_1", "mock_value_2"],
            "pi_b": [["mock_value_3", "mock_value_4"]],
            "pi_c": ["mock_value_5", "mock_value_6"]
        }
    }
    return {"zkp_payload": zkp_payload}

async def publisher_node(state: AgentState) -> dict:
    """Вузол 4: Формує JSON і відправляє у RabbitMQ. (Асинхронний вузол)"""
    channel = state["channel"]
    results = state["nlp_results"]
    
    # Формуємо та відправляємо аналітику (завжди)
    analytics_payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "action_type": "post",
        "commitment": state["commitment"],
        "sentiment_score": results["sentiment_score"],
        "toxicity_score": results["toxicity_score"],
        "overall_quality": results["overall_quality"],
        "final_score": state["final_score"],
        "proof_generated": state["is_approved"]
    }
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(analytics_payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="analytics_queue"
    )
    logger.info(" [Publisher] Analytics safely sent to 'analytics_queue'")

    # Відправляємо доказ, якщо він був згенерований
    if state["is_approved"] and state.get("zkp_payload"):
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(state["zkp_payload"]).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="consensus_queue"
        )
        logger.info("  [Publisher] ZKP Mock safely sent to 'consensus_queue'")

    return {"analytics_payload": analytics_payload}

# Умовний перехід (Супервізор)
def approval_router(state: AgentState) -> str:
    """Визначає, куди йти після Scorer'а"""
    if state["is_approved"]:
        return "zkp_generator"
    return "publisher"

# Крок 3 та 4: Збірка та компіляція Графа
workflow = StateGraph(AgentState)

workflow.add_node("nlp_analyst", nlp_analyst_node)
workflow.add_node("scorer", scorer_node)
workflow.add_node("zkp_generator", zkp_generator_node)
workflow.add_node("publisher", publisher_node)

workflow.add_edge(START, "nlp_analyst")
workflow.add_edge("nlp_analyst", "scorer")
workflow.add_conditional_edges("scorer", approval_router)
workflow.add_edge("zkp_generator", "publisher")
workflow.add_edge("publisher", END)

# Готовий граф, який ми будемо викликати
agent_graph = workflow.compile()


# --- RABBITMQ CONSUMER ---
async def consume_rabbitmq_messages():
    """Background task that connects to RabbitMQ and consumes messages."""
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            
            async with connection:
                channel = await connection.channel()
                exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                await queue.bind(exchange)
                await channel.declare_queue("consensus_queue", durable=True)
                await channel.declare_queue("analytics_queue", durable=True)
                logger.info(f" Successfully connected to RabbitMQ. Listening on exchange: '{EXCHANGE_NAME}'")

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            body = json.loads(message.body.decode())
                            post_data = body.get("message", {}).get("data", {})
                            
                            title = post_data.get('title', '')
                            content = post_data.get('content', '')
                            full_text = f"{title}. {content}".strip()
                            extracted_commitment = post_data.get('commitment') or str(post_data.get('postId', 'unknown_id'))
                            logger.info("\n" + "="*60)
                            logger.info(f"  NEW POST RECEIVED (ID: {post_data.get('postId')})")
                            logger.info("="*60)

                            # Крок 5: Запуск графа
                            # Формуємо початковий Стан ("чистий блокнот")
                            initial_state = {
                                "post_text": full_text,
                                "commitment": extracted_commitment,
                                "nlp_results": None,
                                "final_score": None,
                                "is_approved": False,
                                "zkp_payload": None,
                                "analytics_payload": None,
                                "channel": channel  # Передаємо канал для Publisher-вузла
                            }

                            # Граф сам розкидає задачі між агентами і відправляє повідомлення
                            await agent_graph.ainvoke(initial_state)
                            logger.info("="*60 + "\n")

        except (aio_pika.exceptions.AMQPConnectionError, Exception) as e:
            logger.error(f"RabbitMQ connection failed or lost. Retrying in 5 seconds... Error: {e}")
            await asyncio.sleep(5)


# --- FASTAPI LIFESPAN & SETUP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_rabbitmq_messages())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "LangGraph AI Agent System is running and listening to RabbitMQ"}