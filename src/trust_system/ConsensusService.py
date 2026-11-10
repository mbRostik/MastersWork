import asyncio
import json
import logging
import random
import httpx
import aio_pika
from clickhouse_driver import Client

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ConsensusService")

# --- КОНФІГУРАЦІЯ ---
RABBITMQ_URL = "amqp://guest:guest@localhost//"
QUEUE_NAME = "consensus_queue"

# ЗАМІНІТЬ ЦЕЙ URL НА РЕАЛЬНИЙ ЕНДПОІНТ ВАШОГО .NET БЕКЕНДУ
DOTNET_BACKEND_URL = "https://localhost:7075/user/updatereputation"

# --- КОНФІГУРАЦІЯ CLICKHOUSE ---
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "grafana_user"
CLICKHOUSE_PASSWORD = "securepass"
CLICKHOUSE_DB = "analytics_db"

# Ініціалізація клієнта ClickHouse
ch_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)


# Крок 1.5: Оновлення результату в ClickHouse (Фоновий потік)
def update_clickhouse_consensus(commitment: str, status: str):
    """
    Оновлює поле consensus_result у таблиці post_analytics.
    У ClickHouse це робиться через механізм мутацій (ALTER TABLE).
    """
    try:
        query = """
            ALTER TABLE post_analytics 
            UPDATE consensus_result = %(status)s 
            WHERE commitment = %(commitment)s
        """
        # Виконуємо запит із параметрами, щоб уникнути SQL-ін'єкцій
        ch_client.execute(query, {'status': status, 'commitment': commitment})
        logger.info(f" 💾 ClickHouse updated! Consensus result for {commitment} set to '{status}'.")
    except Exception as e:
        logger.error(f" ❌ Failed to update ClickHouse: {e}")

        
# Крок 2: Імітація агента-валідатора
def verify_proof_by_validator(proof: dict, validator_id: int) -> bool:
    """
    Імітує роботу одного вузла-валідатора.
    Перевіряє наявність commitment, public_signal == True, 
    а також має 10% шанс відхилити доказ (імітація збою або зловмисника).
    """
    # Перевірка базової структури доказу
    if "commitment" not in proof or proof.get("public_signal") is not True:
        logger.warning(f"  [Validator {validator_id}] Invalid proof structure.")
        return False
    
    # 10% ймовірність того, що валідатор "зламався" або навмисно голосує проти
    if random.random() < 0.10:
        logger.warning(f"  [Validator {validator_id}] Byzanite failure! Voting False.")
        return False
        
    return True


# Крок 5: Оновлення репутаційного балу (HTTP-запит)
async def update_reputation(commitment: str):
    """Відправляє анонімний запит на .NET бекенд для нарахування репутації."""
    payload = {
        "commitment": commitment,
        "status": "APPROVED"
    }
    
    logger.info(f" 🌐 Sending HTTP POST to {DOTNET_BACKEND_URL} with payload: {payload}")
    
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(DOTNET_BACKEND_URL, json=payload, timeout=5.0)
            
            # Перевіряємо, чи бекенд відповів успішно (200 OK або 204 No Content)
            if response.status_code in (200, 204):
                logger.info(" ✅ Reputation successfully updated on the .NET backend!")
            else:
                logger.error(f" ❌ Failed to update reputation. Backend returned: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f" ❌ HTTP Request failed (is your .NET backend running?): {e}")


# Крок 1, 3 та 4: Основний процес обробки та голосування
async def process_zkp_message(zkp_payload: dict):
    commitment = zkp_payload.get("commitment", "UNKNOWN")
    logger.info("\n" + "="*50)
    logger.info(f" 🔍 NEW PROOF RECEIVED FOR CONSENSUS")
    logger.info(f" Commitment: {commitment}")
    logger.info("="*50)

    # Крок 3: Процес "Голосування" (5 валідаторів)
    votes = []
    logger.info(" ⚖️ Starting validation process (5 Agents)...")
    
    for i in range(1, 6):
        vote = verify_proof_by_validator(zkp_payload, validator_id=i)
        votes.append(vote)
        result_text = "✅ TRUE" if vote else "❌ FALSE"
        logger.info(f"  -> Validator {i} voted: {result_text}")

    # Крок 4: Перевірка консенсусу (Правило 2/3)
    positive_votes = votes.count(True)
    logger.info("-" * 50)
    logger.info(f" 📊 Voting Results: {positive_votes}/5 Positive")

    # Алгоритм PBFT: для 5 вузлів, 4 або 5 голосів "за" означає досягнення консенсусу
    if positive_votes >= 4:
        logger.info(" 🟢 CONSENSUS REACHED! Proceeding to reputation update.")
        
        # 1. Оновлюємо бекенд .NET
        await update_reputation(commitment)
        
        # 2. Оновлюємо запис у ClickHouse (статус APPROVED)
        await asyncio.to_thread(update_clickhouse_consensus, commitment, "APPROVED")
    else:
        logger.warning(" 🔴 CONSENSUS FAILED! Not enough positive votes. Proof rejected.")
        await asyncio.to_thread(update_clickhouse_consensus, commitment, "REJECTED")

    logger.info("="*50 + "\n")


# Крок 1: Каркас та підключення до RabbitMQ
async def main():
    """Слухає consensus_queue і запускає процес голосування для кожного повідомлення."""
    while True:
        try:
            ch_client.execute('SELECT 1')

            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                
                # Декларуємо чергу, щоб вона точно існувала
                queue = await channel.declare_queue(QUEUE_NAME, durable=True)
                
                logger.info(f"🚀 ConsensusService running! Listening for ZKP mocks on '{QUEUE_NAME}'...")

                # Витягуємо повідомлення з черги
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            # Парсимо JSON
                            zkp_payload = json.loads(message.body.decode())
                            
                            # Віддаємо доказ на голосування
                            await process_zkp_message(zkp_payload)

        except (aio_pika.exceptions.AMQPConnectionError, Exception) as e:
            logger.error(f"RabbitMQ connection lost. Retrying in 5 seconds... Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Запуск асинхронного циклу
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down ConsensusService...")