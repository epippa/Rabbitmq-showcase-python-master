import os

class Settings:
    rabbitmq_host: str = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbitmq_port: int = int(os.getenv("RABBITMQ_PORT", 5672))
    rabbitmq_username: str = os.getenv("RABBITMQ_USERNAME", "guest")
    rabbitmq_password: str = os.getenv("RABBITMQ_PASSWORD", "guest")

    exchanger: str = os.getenv("RABBITMQ_EXCHANGER", "amq.direct")
    queue_name_to_first_service: str = os.getenv("QUEUE_NAME_TO_FIRST_SERVICE", "notify_user")
    routing_key_to_first_service: str = os.getenv("ROUTING_KEY_TO_FIRST_SERVICE", "user.mailing")

    queue_name_to_second_service: str = os.getenv("QUEUE_NAME_TO_SECOND_SERVICE", "changebalance_orders")
    routing_key_to_second_service: str = os.getenv("ROUTING_KEY_TO_SECOND_SERVICE", "orders.checkout")
