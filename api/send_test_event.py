import asyncio
from broker import a_publish_to_rabbitmq

event_data = {
    "id": 1,
    "user_id": 2,
    "email": "test@example.com",
    "message": "Benvenuto su RabbitMQ!"
}

async def main():
    await a_publish_to_rabbitmq(
        queue_name="notify_user",
        exchanger="amq.direct",
        routing_key="user.mailing",
        data=event_data
    )

if __name__ == "__main__":
    asyncio.run(main())
