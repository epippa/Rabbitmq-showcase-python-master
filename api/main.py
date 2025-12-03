import json, os, time

import pika
from fastapi import FastAPI, HTTPException
from opentelemetry import propagate, trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pika.exceptions import AMQPConnectionError
from opentelemetry.trace import SpanKind

# Inizializza TracerProvider con il nome del servizio "api"
resource = Resource.create({SERVICE_NAME: "api"})
provider = TracerProvider(resource=resource)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",  # nome del container Jaeger
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

app = FastAPI()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))


def publish(queue_name: str, message: dict, span_name: str):
    """Publish a message with simple retry so startup does not fail if broker is slow."""
    with tracer.start_as_current_span(span_name, kind=SpanKind.CLIENT) as span:
        span.set_attribute("event_kind", "CALL")
        span.set_attribute("service", "api")
        span.set_attribute("meta", f"queue:{queue_name}")
        span.set_attribute("peer.service", queue_name)

        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination_kind", "queue")
        span.set_attribute("messaging.destination", queue_name)
        span.set_attribute("messaging.operation", "publish")
        
        headers = {}
        propagate.inject(headers)
        last_err = None
        for _ in range(5):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
                )
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=json.dumps(message).encode("utf-8"),
                    properties=pika.BasicProperties(headers=headers),
                )
                connection.close()
                return
            except AMQPConnectionError as exc:
                last_err = exc
                time.sleep(2)
        raise HTTPException(
            status_code=503,
            detail=f"RabbitMQ unavailable at {RABBITMQ_HOST}:{RABBITMQ_PORT}: {last_err}",
        )

@app.get("/")
async def root():
    return {"message": "API service is running"}


@app.post("/service1")
async def call_service1(message: dict):
    publish(queue_name="service1", message=message, span_name="publish_to_service1")
    return {"status": "Message sent to service1"}


@app.post("/service2")
async def call_service2(message: dict):
    publish(queue_name="service2", message=message, span_name="publish_to_service2")
    return {"status": "Message sent to service2"}

@app.get("/health")
async def health():
    return {"status": "ok"}

# Emit a startup span so the service registers in Jaeger even before handling traffic.
with tracer.start_as_current_span("api.startup"):
    pass
