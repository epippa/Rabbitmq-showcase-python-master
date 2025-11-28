from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

import asyncio
import aio_pika
from aio_pika import ExchangeType
from aio_pika.exceptions import AMQPConnectionError
from aio_pika.abc import AbstractIncomingMessage

import json
from config import Settings

# Tracing setup
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({SERVICE_NAME: "service2"}))
)
jaeger_exporter = JaegerExporter(agent_host_name="template_jaeger", agent_port=6831)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))

settings = Settings()

async def publish_to_queue(message: AbstractIncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        print(f"[service2] Received message: {data}")

async def start():
    while True:
        try:
            print("[service2] Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(
                f"amqp://guest:guest@{settings.rabbitmq_host}:{settings.rabbitmq_port}/"
            )
            print("[service2] Connected to RabbitMQ")
            channel = await connection.channel()
            exchange = await channel.declare_exchange(settings.exchanger, ExchangeType.DIRECT, durable=True)
            queue = await channel.declare_queue(settings.queue_name_to_second_service, durable=True)
            await queue.bind(exchange, routing_key=settings.routing_key_to_second_service)
            print(f"[service2] Waiting for messages on queue '{settings.queue_name_to_second_service}'...")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await publish_to_queue(message)
        except AMQPConnectionError as e:
            print(f"[service2] AMQP Connection failed: {e}. Retry in 2s...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[service2] Unexpected error: {e}. Retry in 2s...")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(start())
