from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

MessageHandler = Callable[
    [AbstractIncomingMessage],
    Awaitable[None],
]


class RabbitMQClient:
    def __init__(
        self,
        url: str = "guest:guest@localhost:5672"
    ):
        self.url = url
        self.connection = None
        self.channel = None

    async def connect(self) -> None:
        if self.connection and not self.connection.is_closed:
            return

        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()

    async def declare_queue(self, name: str):
        if self.channel is None:
            raise RuntimeError("RabbitMQ не подключен")

        return await self.channel.declare_queue(
            name,
            durable=True,
        )

    async def publish(
        self,
        queue: str,
        body: bytes | str,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
    ) -> None:
        if self.channel is None:
            raise RuntimeError("RabbitMQ не подключен")

        if isinstance(body, str):
            body = body.encode("utf-8")

        # AMQP headers не должны содержать None.
        clean_headers = {
            key: value
            for key, value in (headers or {}).items()
            if value is not None
        }

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=body,
                headers=clean_headers,
                correlation_id=correlation_id,
                reply_to=reply_to,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=queue,
        )

    async def consume(
        self,
        queue_name: str,
        handler: MessageHandler,
        *,
        prefetch_count: int = 1,
    ) -> None:
        if self.channel is None:
            raise RuntimeError("RabbitMQ не подключен")

        await self.channel.set_qos(
            prefetch_count=prefetch_count,
        )

        queue = await self.declare_queue(queue_name)

        logger.info(f"Consuming queue: {queue_name}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    async with message.process(requeue=False):
                        await handler(message)

                except Exception as e:
                    logger.exception(f"Failed to process message from {queue_name}:\n{e}")

    async def close(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

        self.channel = None
        self.connection = None
