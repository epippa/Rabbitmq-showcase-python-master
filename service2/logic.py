import json
import logging

from aio_pika import Message
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

async def change_balance(message: Message):
    """
    Process the order checkout message and change the user's balance (with tracing).
    """
    async with message.process():  # acknowledge message after processing
        data = json.loads(message.body.decode())
        user = data.get("user_id")
        # Extract tracing context from message headers and continue trace
        headers = getattr(message, "headers", {}) or {}
        context = TraceContextTextMapPropagator().extract(headers)
        # Start a span for message consumption/processing
        with tracer.start_as_current_span("change_balance", context=context, kind=SpanKind.CONSUMER):
            logger.warning(f"[*] - User's balance with id {user} changed. Order checkouted")
