from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.pika import PikaInstrumentor

from functools import lru_cache
from typing import Annotated

import uvicorn
from broker import a_publish_to_rabbitmq, publish_to_rabbitmq
from config import Settings
from fastapi import Depends, FastAPI

service_name = "api"
# Setup tracing BEFORE app is created
trace.set_tracer_provider(
    TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
)
jaeger_exporter = JaegerExporter(agent_host_name="template_jaeger", agent_port=6831)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(jaeger_exporter))
PikaInstrumentor().instrument()

# Create the FastAPI app
description = """
Microservice boilerplate

## Usage
- Pass foo data to any of API's endpoints (You can use foo data from down below)
- Look up to your terminal

{ 
"id": 50, 
"user_id": 12, 
"title": "Hello world", 
"description": "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book.", 
"category_id": 4, 
"price": "5000" 
}
"""
settings = Settings()
app = FastAPI(description=description)

# Instrument FastAPI and Requests after app is created
FastAPIInstrumentor.instrument_app(app)
RequestsInstrumentor().instrument()


@lru_cache
def get_settings():
    return Settings()


@app.post("/api/user/subscribe")
def subscribe_user(data: dict, settings: Annotated[Settings, Depends(get_settings)]):
    # Publish message to first service (synchronous)
    publish_to_rabbitmq(
        queue_name=settings.queue_name_to_first_service,
        exchanger=settings.exchanger,
        routing_key=settings.routing_key_to_first_service,
        data=data
    )
    return {"detail": "User subscribed."}


@app.get("/test-trace")
def test_trace():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("test-span"):
        return {"message": "Tracing OK"}


@app.post("/api/order/checkout")
async def order_checkout(data: dict, settings: Annotated[Settings, Depends(get_settings)]):
    # Publish message to second service (asynchronous)
    await a_publish_to_rabbitmq(
        queue_name=settings.queue_name_to_second_service,
        exchanger=settings.exchanger,
        routing_key=settings.routing_key_to_second_service,
        data=data
    )
    return {"detail": "Order created."}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
