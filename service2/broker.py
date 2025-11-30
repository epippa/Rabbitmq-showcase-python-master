import aio_pika
from logic import change_balance

QUEUE_NAME_TO_SECOND_SERVICE = "changebalance_orders"

async def start_consuming():
    """Connect to RabbitMQ and consume messages for service2."""
    connection = await aio_pika.connect(host="rabbitmq")
    channel = await connection.channel()
    queue = await channel.declare_queue(QUEUE_NAME_TO_SECOND_SERVICE, durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            # Delegate message processing to logic (which handles tracing and ack)
            await change_balance(message)
