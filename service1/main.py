from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import asyncio
import aio_pika
from aio_pika import ExchangeType
from aio_pika.exceptions import AMQPConnectionError
from aio_pika.abc import AbstractIncomingMessage
import json

from config import Settings

# Tracing setup for service1
trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create({SERVICE_NAME: "service1"})
    )
)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)
# Use W3C Trace Context propagator globally
set_global_textmap(TraceContextTextMapPropagator())
# Get a tracer for creating spans in this service
tracer = trace.get_tracer(__name__)

settings = Settings()

async def publish_to_queue(message: AbstractIncomingMessage):
    """Process a single incoming RabbitMQ message (with tracing)."""
    async with message.process():  # auto-acknowledge after processing
        # Extract tracing headers from the message and continue the trace
        headers = getattr(message, "headers", {}) or {}
        context = TraceContextTextMapPropagator().extract(headers)
        # Start a consumer span for this message processing
        with tracer.start_as_current_span("process message", context=context, kind=SpanKind.CONSUMER) as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", settings.queue_name_to_first_service)
            span.set_attribute("messaging.rabbitmq.routing_key", message.routing_key)
            data = json.loads(message.body.decode())
            print(f"[service1] Received message: {data}")

async def start():
    print("[service1] Waiting for messages... (connecting to RabbitMQ)")
    while True:
        try:
            # Connect to RabbitMQ (uses environment variables for host)
            connection = await aio_pika.connect_robust(
                f"amqp://guest:guest@{settings.rabbitmq_host}:{settings.rabbitmq_port}/"
            )
            channel = await connection.channel()
            # Declare exchange and queue (idempotent) and bind them
            await channel.declare_exchange(settings.exchanger, ExchangeType.DIRECT, durable=True)
            queue = await channel.declare_queue(settings.queue_name_to_first_service, durable=True)
            await queue.bind(settings.exchanger, routing_key=settings.routing_key_to_first_service)
            # Consume messages continuously
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await publish_to_queue(message)
        except AMQPConnectionError as e:
            print(f"[ERROR] AMQP Connection Error in service1: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[ERROR] Unexpected exception in service1: {e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(start())
