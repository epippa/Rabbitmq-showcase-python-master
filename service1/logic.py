import json
import logging
from opentelemetry import trace
from pika.spec import Basic, BasicProperties
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

def notify_user(channel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    with tracer.start_as_current_span("notify_user", kind=SpanKind.CONSUMER):
        data = json.loads(body.decode())
        logger.warning(f"[Service1] Notification sent to user {data.get('user_id')}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
