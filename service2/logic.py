import json
import logging
from aio_pika import Message
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.propagate import TraceContextTextMapPropagator

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

async def change_balance(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():  # ack automatico a fine blocco
        data = json.loads(message.body.decode())
        user = data.get("user_id")

        # Estrae il contesto di traccia dagli header del messaggio (se presenti)
        msg_headers = message.headers  # oppure message.properties.headers a seconda della versione di aio_pika
        ctx = TraceContextTextMapPropagator().extract(msg_headers)

        # Avvia uno span con contesto estratto, così che sia collegato al producer
        with tracer.start_as_current_span("change_balance", context=ctx, kind=trace.SpanKind.SERVER) as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", "changebalance_orders")
            span.set_attribute("messaging.operation", "process")  # es. tipo di operazione
            logger.warning(f"[*] - User's balance with id {user} changed. Order checked out.")
