import asyncio
import json
import logging
import pika
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from config import Settings

settings = Settings()

# Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("service1")

# OpenTelemetry Tracer
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({SERVICE_NAME: "service1"}))
)
tracer = trace.get_tracer(__name__)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(JaegerExporter(agent_host_name="jaeger", agent_port=6831))
)


def callback(ch, method, properties, body):
    with tracer.start_as_current_span("service1-process-message"):
        data = json.loads(body)
        logger.info(f"[service1] Received message: {data}")


def start():
    logger.debug("start() function entered")
    while True:
        try:
            logger.debug("Attempting connection to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.rabbitmq_host))
            channel = connection.channel()

            channel.exchange_declare(exchange=settings.exchanger, exchange_type="direct", durable=True)
            channel.queue_declare(queue=settings.queue_name_to_first_service, durable=True)
            channel.queue_bind(
                exchange=settings.exchanger,
                queue=settings.queue_name_to_first_service,
                routing_key=settings.routing_key_to_first_service,
            )

            channel.basic_consume(queue=settings.queue_name_to_first_service, on_message_callback=callback, auto_ack=True)
            logger.info("[service1] Waiting for messages...")
            channel.start_consuming()
        except Exception as e:
            logger.error(f"AMQP Connection Error: {e}")
            asyncio.sleep(2)


if __name__ == "__main__":
    logger.debug("main.py is running")
    start()
