import os
import asyncio
from aio_pika import connect_robust, Message


async def main():
    host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    conn = await connect_robust(f"amqp://guest:guest@{host}:{port}/")
    channel = await conn.channel()

    # Notifica utente (service1)
    await channel.default_exchange.publish(
        Message(body=b'{"id": 1, "user_id": 2, "description": "notify-user"}'),
        routing_key="notify_key"
    )

    # Ordine checkout (service2)
    await channel.default_exchange.publish(
        Message(body=b'{"id": 99, "user_id": 5, "description": "order-checkout"}'),
        routing_key="order_key"
    )

    print("Messaggi inviati.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
