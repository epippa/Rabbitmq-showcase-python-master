import json
import logging
from aio_pika import Message
from opentelemetry import trace
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

async def change_balance(message: Message):
    async with message.process():
        with tracer.start_as_current_span("change_balance", kind=SpanKind.CONSUMER):
            data = json.loads(message.body.decode())
            logger.warning(f"[Service2] Balance changed for user {data.get('user_id')}")
