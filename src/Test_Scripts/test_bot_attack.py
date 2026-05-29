import asyncio
import json
import uuid
import logging
import aio_pika
import hashlib
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

RABBITMQ_URL = "amqp://guest:guest@localhost//"
EXCHANGE_NAME = "Posts.Application.UseCases.Handlers.Creation:IPostCreated_PublishEvent_From_PostWebApi"

def generate_spam_payload(index: int):
    post_id = str(uuid.uuid4())
    
    # Генеруємо реалістичний хеш і додаємо префікс
    raw_data = f"{post_id}-spam-{time.time()}-{index}"
    commitment_hash = hashlib.sha256(raw_data.encode()).hexdigest()[:40] # Беремо перші 40 символів
    commitment = f"bot_{commitment_hash}"
    
    return {
        "message": {
            "data": {
                "postId": post_id,
                "title": f"Spam Post #{index}",
                "content": "Buy cheap watches! Click here! This is an automated spam message to trigger the bot alert.",
                "commitment": commitment
            }
        }
    }

async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
        
        logging.info("🚨 INITIATING BOT ATTACK (200 posts instantly)...")
        
        for i in range(1, 201):
            payload = generate_spam_payload(i)
            message_body = json.dumps(payload).encode()
            
            await exchange.publish(
                aio_pika.Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=""
            )
            
            if i % 50 == 0:
                logging.info(f"💥 {i} spam messages sent...")
                
        logging.info("✅ Bot attack completed! Check your Grafana dashboard for alerts.")

if __name__ == "__main__":
    asyncio.run(main())