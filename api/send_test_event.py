import asyncio
from aio_pika import connect_robust, Message
import json

async def main():
    # Connessione al broker RabbitMQ sul giusto host/porta:
    conn = await connect_robust("amqp://guest:guest@localhost:5673/")
    chan = await conn.channel()
    msg = {
        "id": 1,
        "user_id": 2,
        "title": "test",
        "description": "test",
        "category_id": 1,
        "price": "100"
    }
    await chan.default_exchange.publish(
        Message(body=json.dumps(msg).encode()),
        routing_key="user.mailing"
    )
    print("[send_test_event] Sent test message")
    await conn.close()

asyncio.run(main())
