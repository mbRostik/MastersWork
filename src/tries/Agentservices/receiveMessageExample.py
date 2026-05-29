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

# Suppress minor huggingface warnings for cleaner output
warnings.filterwarnings('ignore')

# Setup logging to view the messages in the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RabbitMQConsumer")

# --- CONFIGURATION ---
RABBITMQ_URL = "amqp://guest:guest@localhost//"
EXCHANGE_NAME = "Posts.Application.UseCases.Handlers.Creation:IPostCreated_PublishEvent_From_PostWebApi"
QUEUE_NAME = "python_post_subscriber_queue"

# Absolute Windows path to your models folder
BASE_PATH = r"D:\OverComeItBunchOfProjects\Personal\master\development\trust_system\models"


# --- AI ANALYZER CLASS ---
class TextAnalyzer:
    def __init__(self):
        # Automatically detect GPU
        self.device = 0 if torch.cuda.is_available() else -1
        device_name = "GPU" if self.device == 0 else "CPU"
        logger.info(f"Loading AI weights locally on {device_name} (with remote tokenizers)...")

        # 1. Sentiment Pipeline
        sent_dir = os.path.join(BASE_PATH, "sentiment")
        sent_model = AutoModelForSequenceClassification.from_pretrained(sent_dir)
        sent_tok = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment-latest")
        self.sentiment_pipe = pipeline("sentiment-analysis", model=sent_model, tokenizer=sent_tok, device=self.device)

        # 2. Toxicity Pipeline
        tox_dir = os.path.join(BASE_PATH, "toxicity")
        tox_model = AutoModelForSequenceClassification.from_pretrained(tox_dir)
        tox_tok = AutoTokenizer.from_pretrained("unitary/toxic-bert")
        self.toxicity_pipe = pipeline("text-classification", model=tox_model, tokenizer=tox_tok, device=self.device)

        # 3. Quality Pipeline
        qual_dir = os.path.join(BASE_PATH, "quality")
        qual_model = AutoModelForSequenceClassification.from_pretrained(qual_dir)
        qual_tok = AutoTokenizer.from_pretrained("textattack/roberta-base-CoLA")
        self.quality_pipe = pipeline("text-classification", model=qual_model, tokenizer=qual_tok, device=self.device)
        
        logger.info("✅ All AI Models loaded successfully into memory!")

    def analyze(self, text):
        if not isinstance(text, str) or not text.strip():
            return {"sentiment_score": 0.5, "toxicity_score": 0.0, "overall_quality": 0.0}

        # --- Sentiment Analysis ---
        sent_res = self.sentiment_pipe(text)[0]
        label = sent_res['label'].lower()
        if label == 'positive':
            sentiment_score = sent_res['score']
        elif label == 'negative':
            sentiment_score = 1.0 - sent_res['score']
        else:
            sentiment_score = 0.5  # Neutral

        # --- Toxicity Analysis ---
        tox_res = self.toxicity_pipe(text)[0]
        if tox_res['label'].lower() == 'toxic':
            toxicity_score = tox_res['score']
        else:
            toxicity_score = 1.0 - tox_res['score']

        # --- Quality Analysis ---
        qual_res = self.quality_pipe(text)[0]
        if qual_res['label'] == 'LABEL_1':
            quality_score = qual_res['score']
        else:
            quality_score = 1.0 - qual_res['score']

        return {
            "sentiment_score": round(float(sentiment_score), 4),
            "toxicity_score": round(float(toxicity_score), 4),
            "overall_quality": round(float(quality_score), 4)
        }

# Initialize the analyzer globally so it only loads once on startup
analyzer = TextAnalyzer()


# --- RABBITMQ CONSUMER ---
async def consume_rabbitmq_messages():
    """Background task that connects to RabbitMQ and consumes messages."""
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            
            async with connection:
                channel = await connection.channel()
                
                exchange = await channel.declare_exchange(
                    EXCHANGE_NAME, 
                    aio_pika.ExchangeType.FANOUT, 
                    durable=True
                )
                
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                await queue.bind(exchange)
                
                logger.info(f"🚀 Successfully connected to RabbitMQ. Listening on exchange: '{EXCHANGE_NAME}'")

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            body = json.loads(message.body.decode())
                            
                            mt_message = body.get("message", {})
                            post_data = mt_message.get("data", {})

                            title = post_data.get('title', '')
                            content = post_data.get('content', '')
                            
                            # Combine title and content for the AI to analyze
                            full_text = f"{title}. {content}".strip()

                            # Run the AI Analysis
                            ai_results = analyzer.analyze(full_text)

                            # Log everything to the console
                            logger.info("\n" + "="*50)
                            logger.info(" 📩 NEW POST RECEIVED & ANALYZED")
                            logger.info(f"Post ID:        {post_data.get('postId')}")
                            logger.info(f"Text:           {full_text}")
                            logger.info("-" * 50)
                            logger.info(f"Sentiment:      {ai_results['sentiment_score']} (Closer to 1.0 = Positive)")
                            logger.info(f"Toxicity:       {ai_results['toxicity_score']} (Closer to 1.0 = Toxic)")
                            logger.info(f"Grammar/Quality:{ai_results['overall_quality']} (Closer to 1.0 = Good)")
                            logger.info("="*50 + "\n")

        except (aio_pika.exceptions.AMQPConnectionError, Exception) as e:
            logger.error(f"RabbitMQ connection failed or lost. Retrying in 5 seconds... Error: {e}")
            await asyncio.sleep(5)


# --- FASTAPI LIFESPAN & SETUP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This runs when FastAPI starts up
    task = asyncio.create_task(consume_rabbitmq_messages())
    yield
    # This runs when FastAPI shuts down
    task.cancel()

# Initialize FastAPI
app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "Python API is running, AI is loaded locally, and listening to RabbitMQ"}