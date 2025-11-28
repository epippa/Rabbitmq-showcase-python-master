import json
import aio_pika
from aio_pika import ExchangeType, Message
from config import Settings

settings = Settings()

async def a_publish_to_rabbitmq(queue_name: str, exchanger: str, routing_key: str, data: dict):
    connection = await aio_pika.connect_robust("amqp://guest:guest@template_rabbitmq/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(exchanger, ExchangeType.DIRECT, durable=True)
        await channel.declare_queue(queue_name, durable=True)
        await exchange.publish(
            Message(body=json.dumps(data).encode()),
            routing_key=routing_key
        )
