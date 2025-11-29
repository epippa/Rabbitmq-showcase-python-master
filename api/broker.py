import json
import os

import aio_pika
import pika
from opentelemetry import trace
from opentelemetry.propagate import inject
from config import Settings

# Load RabbitMQ connection parameters from environment (with defaults)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "template_rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))

settings = Settings()

def publish_to_rabbitmq(queue_name: str, exchanger: str, routing_key: str, data: dict):
    # Inietta il contesto di tracing negli header (standard 'traceparent')
    headers = {}
    inject(headers)

    # Connessione a RabbitMQ
    params = pika.ConnectionParameters(
        host=settings.rabbitmq_host,
        port=settings.rabbitmq_port,
        credentials=pika.PlainCredentials("guest", "guest")
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Assicurati che la coda sia bindata
    channel.queue_bind(queue=queue_name, exchange=exchanger, routing_key=routing_key)

    # Pubblica il messaggio con headers di tracing
    properties = pika.BasicProperties(
        headers=headers,
        content_type="application/json"
    )
    channel.basic_publish(
        exchange=exchanger,
        routing_key=routing_key,
        body=json.dumps(data).encode(),
        properties=properties
    )
    connection.close()

async def a_publish_to_rabbitmq(queue_name: str, exchanger: str, routing_key: str, data: dict) -> None:
    # Get current trace context
    current_span = trace.get_current_span()
    ctx = current_span.get_span_context()
    trace_id_hex = format(ctx.trace_id, '032x')
    span_id_hex = format(ctx.span_id, '016x')
    headers = {'trace_id': trace_id_hex, 'span_id': span_id_hex}

    # Connect to RabbitMQ and publish message with trace headers
    connection = await aio_pika.connect_robust(host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest")
    channel = await connection.channel()
    exchange = await channel.get_exchange(exchanger)
    try:
        queue = await channel.get_queue(queue_name)
    except aio_pika.exceptions.ChannelNotFoundEntity:
        queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(exchange, routing_key)
    message = aio_pika.Message(
        json.dumps(data).encode(),
        content_type="application/json",
        headers=headers
    )
    await exchange.publish(message, routing_key)
    await connection.close()
