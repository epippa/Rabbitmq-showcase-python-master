from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.trace import SpanContext, NonRecordingSpan, set_span_in_context, SpanKind, TraceFlags, TraceState

import os
import asyncio
import aio_pika
from aio_pika import ExchangeType
from aio_pika.exceptions import AMQPConnectionError
from aio_pika.abc import AbstractIncomingMessage

import json
from config import Settings
from logic import change_balance  # import the business logic for service2

print("[DEBUG] service2 main.py is running")

# Tracing setup
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({SERVICE_NAME: "service2"}))
)
jaeger_exporter = JaegerExporter(agent_host_name="template_jaeger", agent_port=6831)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))
tracer = trace.get_tracer(__name__)

settings = Settings()
# RabbitMQ connection parameters from env
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "template_rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))


async def process_message(message: AbstractIncomingMessage):
    # Process the incoming message with trace context propagation
    headers = message.headers or {}
    if 'trace_id' in headers and 'span_id' in headers:
        trace_id = int(headers['trace_id'], 16)
        span_id = int(headers['span_id'], 16)
        parent_context = SpanContext(trace_id=trace_id, span_id=span_id, is_remote=True, trace_flags=TraceFlags(1))
        parent_span = NonRecordingSpan(parent_context)
        ctx = set_span_in_context(parent_span)
    else:
        ctx = None

    # Start a SERVER span for the received message and execute business logic
    if ctx:
        with tracer.start_as_current_span("RECEIVE service2", context=ctx, kind=SpanKind.SERVER):
            print(f"[service2] Received message: {json.loads(message.body.decode())}")
            await change_balance(message)
    else:
        with tracer.start_as_current_span("RECEIVE service2", kind=SpanKind.SERVER):
            print(f"[service2] Received message: {json.loads(message.body.decode())}")
            await change_balance(message)


async def start():
    print("[DEBUG] service2 start() entered")
    while True:
        try:
            print("[DEBUG] Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest")
            print("[DEBUG] RabbitMQ connection established")

            channel = await connection.channel()
            exchange = await channel.declare_exchange(settings.exchanger, ExchangeType.DIRECT, durable=True)
            print(f"[DEBUG] Exchange declared: {exchange.name}")

            queue = await channel.declare_queue(settings.queue_name_to_second_service, durable=True)
            print(f"[DEBUG] Queue declared: {queue.name}")

            await queue.bind(exchange, routing_key=settings.routing_key_to_second_service)
            print(f"[DEBUG] Queue bound to exchange with routing key '{settings.routing_key_to_second_service}'")

            print("[service2] Waiting for messages...")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    # Note: message acknowledgment is handled in change_balance()
                    await process_message(message)

        except AMQPConnectionError as e:
            print(f"[ERROR] AMQP Connection Error (service2): {e}")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[ERROR] Unexpected exception in service2: {e}")
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(start())
