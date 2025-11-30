from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

import asyncio

from broker import start_consuming

# Tracing setup for service2
trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create({SERVICE_NAME: "service2"})
    )
)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)
# Set global propagator for trace context
set_global_textmap(TraceContextTextMapPropagator())
tracer = trace.get_tracer(__name__)

if __name__ == "__main__":
    # Run the consuming loop (consumes messages and processes them in logic.py)
    with tracer.start_as_current_span("service2.run"):
        asyncio.run(start_consuming())
