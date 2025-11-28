import time

import pika
from pika.exceptions import AMQPConnectionError

from logic import notify_user

QUEUE_NAME_TO_FIRST_SERVICE = "notify_user"


def connect_routes(channel):
    channel.basic_consume(QUEUE_NAME_TO_FIRST_SERVICE, notify_user)


def start_consuming():
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters("template_rabbitmq"))
            channel = connection.channel()
            channel.queue_declare(QUEUE_NAME_TO_FIRST_SERVICE)
            connect_routes(channel)
            channel.start_consuming()
        except AMQPConnectionError:
            time.sleep(1)
        finally:
            if connection and not connection.is_closed:
                connection.close()
