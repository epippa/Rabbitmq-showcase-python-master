import json
import aio_pika
import pika

from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Initialize tracer for the API service
tracer = trace.get_tracer(__name__)

def publish_to_rabbitmq(queue_name: str, exchanger: str, routing_key: str, data: dict) -> None:
    """Synchronous publish using pika (traced automatically by PikaInstrumentor)."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    # Ensure the queue is bound to the exchange with the routing key
    channel.queue_bind(queue_name, exchanger, routing_key)
    # This publish will be captured by OpenTelemetry Pika instrumentation (with context propagation)
    channel.basic_publish(exchanger, routing_key, json.dumps(data).encode())
    connection.close()

async def a_publish_to_rabbitmq(queue_name: str, exchanger: str, routing_key: str, data: dict) -> None:
    """Asynchronous publish using aio_pika with manual tracing and context propagation."""
    # Start a new producer span for sending the message
    with tracer.start_as_current_span("send to RabbitMQ", kind=SpanKind.PRODUCER) as span:
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination", queue_name)
        span.set_attribute("messaging.rabbitmq.routing_key", routing_key)
        span.set_attribute("peer.service", "service2")  # target service is service2
        # Inject current trace context into message headers
        headers: dict = {}
        TraceContextTextMapPropagator().inject(headers)
        # Connect to RabbitMQ and publish message with headers
        connection = await aio_pika.connect_robust(host="rabbitmq")
        channel = await connection.channel()
        exchange = await channel.get_exchange(exchanger)
        # Declare queue if not already existing, then bind (idempotent operations)
        try:
            queue = await channel.get_queue(queue_name)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key)
        # Publish message with trace context headers
        message = aio_pika.Message(
            json.dumps(data).encode(),
            content_type="application/json",
            headers=headers
        )
        await exchange.publish(message, routing_key)
        await connection.close()
