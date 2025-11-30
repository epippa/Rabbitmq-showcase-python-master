from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.pika import PikaInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from functools import lru_cache
from typing import Annotated

from broker import a_publish_to_rabbitmq, publish_to_rabbitmq
from config import Settings
from fastapi import Depends, FastAPI

# Setup OpenTelemetry tracing (TracerProvider with service name) and Jaeger exporter
trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create({SERVICE_NAME: "api"})
    )
)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)
# Set global text map propagator to W3C Trace Context for context propagation
set_global_textmap(TraceContextTextMapPropagator())
# Instrument FastAPI and outgoing HTTP calls (requests)
app = FastAPI(description="Microservice boilerplate 🚀")
FastAPIInstrumentor.instrument_app(app)
RequestsInstrumentor().instrument()
# Instrument Pika (RabbitMQ client) for tracing message publishing
PikaInstrumentor().instrument()

settings = Settings()
tracer = trace.get_tracer(__name__)

@lru_cache
def get_settings():
    return Settings()

@app.post("/api/user/subscribe")
def subscribe_user(data: dict, settings: Annotated[Settings, Depends(get_settings)]):
    # Publish message to first service's queue (tracing via Pika instrumentation)
    publish_to_rabbitmq(
        queue_name=settings.queue_name_to_first_service,
        exchanger=settings.exchanger,
        routing_key=settings.routing_key_to_first_service,
        data=data
    )
    return {"detail": "User subscribed."}

@app.get("/test-trace")
def test_trace():
    # Example manual span to test tracing
    with tracer.start_as_current_span("test-span"):
        return {"message": "Tracing OK"}

@app.post("/api/order/checkout")
async def order_checkout(data: dict, settings: Annotated[Settings, Depends(get_settings)]):
    # Publish message to second service's queue (async) with tracing
    await a_publish_to_rabbitmq(
        queue_name=settings.queue_name_to_second_service,
        exchanger=settings.exchanger,
        routing_key=settings.routing_key_to_second_service,
        data=data
    )
    return {"detail": "Order created."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
