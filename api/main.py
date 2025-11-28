from fastapi import FastAPI
from pydantic import BaseModel
import logging
import asyncio

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from broker import a_publish_to_rabbitmq
from config import Settings

settings = Settings()
app = FastAPI()

# Logging setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("api")

# OpenTelemetry Tracer setup
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({SERVICE_NAME: "api"}))
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(JaegerExporter(agent_host_name="jaeger", agent_port=6831))
)
tracer = trace.get_tracer(__name__)


# Request schema
class Event(BaseModel):
    id: int
    user_id: int
    description: str


@app.post("/notify")
async def notify_user(event: Event):
    with tracer.start_as_current_span("api-notify-user"):
        logger.info(f"Received notify event: {event}")
        await a_publish_to_rabbitmq(
            queue_name=settings.queue_name_to_first_service,
            exchanger=settings.exchanger,
            routing_key=settings.routing_key_to_first_service,
            data=event.dict()
        )
        return {"status": "sent to user queue"}


@app.post("/order")
async def order_checkout(event: Event):
    with tracer.start_as_current_span("api-order-checkout"):
        logger.info(f"Received order event: {event}")
        await a_publish_to_rabbitmq(
            queue_name=settings.queue_name_to_second_service,
            exchanger=settings.exchanger,
            routing_key=settings.routing_key_to_second_service,
            data=event.dict()
        )
        return {"status": "sent to order queue"}
