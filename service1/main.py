import json, pika, os, time

import pika
from opentelemetry import propagate, trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pika.exceptions import AMQPConnectionError
from opentelemetry.trace import SpanKind

# Inizializza TracerProvider con il nome del servizio "service1"
resource = Resource.create({SERVICE_NAME: "service1"})
provider = TracerProvider(resource=resource)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

def forward_to_service2(payload: dict) -> None:
    """Publish a message from service1 to the service2 queue, propagating the trace context."""
    with tracer.start_as_current_span(
        "service1_publish_to_service2",
        kind=SpanKind.CLIENT,
    ) as span:
        # Tag per il TUO tool (CALL event)
        span.set_attribute("event_kind", "CALL")
        span.set_attribute("service", "service1")
        span.set_attribute("meta", "queue:service2")
        span.set_attribute("peer.service", "service2")

        # Tag OTEL standard di messaging (facoltativi ma utili)
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination_kind", "queue")
        span.set_attribute("messaging.destination", "service2")
        span.set_attribute("messaging.operation", "publish")

        # Propagazione del contesto nel messaggio
        headers: dict = {}
        propagate.inject(headers)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()
        channel.queue_declare(queue="service2", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="service2",
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(headers=headers),
        )
        connection.close()


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))


def callback(ch, method, properties, body):
    headers = (properties.headers or {}) if properties else {}
    ctx = propagate.extract(headers)
    with tracer.start_as_current_span(
        "service1_process",
        context=ctx,
        kind=SpanKind.SERVER,
    ) as span:
        span.set_attribute("event_kind", "RECEIVE")
        span.set_attribute("service", "service1")
        span.set_attribute("meta", "queue:service1")

        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination_kind", "queue")
        span.set_attribute("messaging.destination", "service1")
        span.set_attribute("messaging.operation", "process")

        data = json.loads(body.decode("utf-8"))
        print(f"service1 received: {data}")
        forward_to_service2(data)
    ch.basic_ack(delivery_tag=method.delivery_tag)



def main():
    print("service1 waiting for messages...")
    # Emit a startup span so the service registers in Jaeger
    with tracer.start_as_current_span("service1.startup"):
        pass
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
            )
            channel = connection.channel()
            channel.queue_declare(queue="service1", durable=True)
            channel.basic_consume(queue="service1", on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except (AMQPConnectionError, OSError) as exc:
            print(f"[service1] RabbitMQ unavailable ({exc}), retrying in 2s...")
            time.sleep(2)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
