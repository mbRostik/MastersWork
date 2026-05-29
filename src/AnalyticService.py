import asyncio
import json
import logging
from datetime import datetime
import aio_pika
from clickhouse_driver import Client

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AnalyticsService")

# --- КОНФІГУРАЦІЯ RABBITMQ ---
RABBITMQ_URL = "amqp://guest:guest@localhost//"
QUEUE_NAME = "analytics_queue"

# --- КОНФІГУРАЦІЯ CLICKHOUSE ---
# Використовуємо порт 9000 (Native interface), як вказано у вашому docker-compose
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "grafana_user"
CLICKHOUSE_PASSWORD = "securepass"
CLICKHOUSE_DB = "analytics_db"

# Створюємо клієнт підключення до ClickHouse
ch_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

# Функція запису в БД (виконується в окремому потоці, щоб не блокувати RabbitMQ)
def insert_into_clickhouse(payload: dict):
    try:
        # Парсимо час із ISO-формату назад в об'єкт datetime
        event_time = datetime.fromisoformat(payload.get("timestamp"))
        
        # Готуємо кортеж даних для запису
        # Як ви і просили, consensus_result залишається None (що в ClickHouse стане NULL)
        row = (
            event_time,                                 # event_time
            payload.get("action_type", "post"),         # event_type
            float(payload.get("overall_quality", 0.0)), # content_quality
            float(payload.get("sentiment_score", 0.5)), # sentiment_score
            float(payload.get("toxicity_score", 0.0)),  # toxicity_score
            payload.get("commitment", ""),              # commitment
            bool(payload.get("proof_generated", False)),# proof_generated
            None                                        # consensus_result (Nullable)
        )

        # Виконуємо запит
        query = """
            INSERT INTO post_analytics (
                event_time, event_type, content_quality, sentiment_score, 
                toxicity_score, commitment, proof_generated, consensus_result
            ) VALUES
        """
        ch_client.execute(query, [row])
        logger.info(f" 💾 Successfully inserted analytics for commitment: {row[5]}")

    except Exception as e:
        logger.error(f" ❌ Failed to insert into ClickHouse: {e}")

# Основний процес обробки повідомлень
async def process_analytics_message(payload: dict):
    logger.info("\n" + "-"*50)
    logger.info(" 📊 NEW ANALYTICS PAYLOAD RECEIVED")
    logger.info(f" Toxicity: {payload.get('toxicity_score')} | Quality: {payload.get('overall_quality')} | Proof: {payload.get('proof_generated')}")
    
    # Викликаємо синхронну функцію ClickHouse у фоновому потоці
    await asyncio.to_thread(insert_into_clickhouse, payload)
    logger.info("-" * 50)

# Каркас та підключення до RabbitMQ
async def main():
    """Слухає analytics_queue і відправляє дані у ClickHouse."""
    while True:
        try:
            # Підключаємося до ClickHouse для перевірки з'єднання (пінг)
            ch_client.execute('SELECT 1')
            logger.info("🟢 Successfully connected to ClickHouse!")

            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                
                # Декларуємо чергу
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                
                logger.info(f"🚀 AnalyticsService running! Listening for logs on '{QUEUE_NAME}'...")

                # Витягуємо повідомлення
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            payload = json.loads(message.body.decode())
                            await process_analytics_message(payload)

        except (aio_pika.exceptions.AMQPConnectionError, Exception) as e:
            logger.error(f"Connection lost (RabbitMQ or ClickHouse). Retrying in 5 seconds... Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down AnalyticsService...")