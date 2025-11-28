import asyncio

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

from logic import change_balance

QUEUE_NAME_TO_SECOND_SERVICE = "changebalance_orders"


async def start_consuming():
    while True:
        try:
            async with await aio_pika.connect(host="template_rabbitmq") as connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(QUEUE_NAME_TO_SECOND_SERVICE)
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        await change_balance(message)
        except AMQPConnectionError:
            await asyncio.sleep(1)
