import asyncio
import json
import uuid
import random
import logging
import aio_pika
import hashlib
import time
from pathlib import Path

from launch_trust_system import load_environment, rabbitmq_connection_options

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

EXCHANGE_NAME = "Posts.Application.UseCases.Handlers.Creation:IPostCreated_PublishEvent_From_PostWebApi"

def generate_payload(is_toxic: bool):
    post_id = str(uuid.uuid4())
    
    # Генеруємо реалістичний 64-символьний хеш (SHA-256)
    raw_data = f"{post_id}-{time.time()}-{random.random()}"
    commitment = hashlib.sha256(raw_data.encode()).hexdigest()
    
    if is_toxic:
        title = "Terrible experience"
        content = "This system is absolute garbage. You are all stupid and I hate everything about this dumb platform!"
    else:
        title = "Great platform"
        content = "I really love using this system. It is wonderful, helpful, and absolutely amazing."

    return {
        "message": {
            "data": {
                "postId": post_id,
                "title": title,
                "content": content,
                "commitment": commitment
            }
        }
    }

async def main():
    environment = load_environment(Path(__file__).resolve().parents[1] / "trust-system.json")
    connection = await aio_pika.connect_robust(**rabbitmq_connection_options(environment), timeout=5)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
        
        # Створюємо список з 40 хороших і 10 поганих постів
        tasks = [False] * 40 + [True] * 10
        random.shuffle(tasks) # Перемішуємо їх
        
        logging.info("🚀 Starting Normal Traffic Simulation (50 posts over 5 minutes)...")
        
        for index, is_toxic in enumerate(tasks, 1):
            payload = generate_payload(is_toxic)
            message_body = json.dumps(payload).encode()
            
            await exchange.publish(
                aio_pika.Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=""
            )
            
            post_type = "🔴 TOXIC" if is_toxic else "🟢 POSITIVE"
            logging.info(f"[{index}/50] Published {post_type} post. Waiting 6 seconds...")
            
            # Чекаємо 6 секунд між постами (50 * 6 = 300 секунд = 5 хвилин)
            await asyncio.sleep(6)
            
        logging.info("✅ Normal traffic test completed!")

if __name__ == "__main__":
    asyncio.run(main())
