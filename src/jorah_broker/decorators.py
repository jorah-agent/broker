# Общий декоратор для подключения к RabbitMQ
import json
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar

from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv
from loguru import logger

from .client import RabbitMQClient

load_dotenv()

P = ParamSpec("P")
R = TypeVar("R")

def rabbitmq_worker(
    *,
    request_queue: str,
    response_queue: str = "",
    attributes: tuple[str, ...] = (),
    optional_attributes: tuple[str, ...] = (),
    rabbitmq_url: str = "guest:guest@localhost:5672",
    skill_name: str = "",
    skill_description: str = "",
    is_skill: bool = True,
    parameters: dict[str, Any] = dict,
    version: str = "1.0.0",
):
    """

    :param request_queue:
    :param response_queue:
    :param attributes:
    :param optional_attributes:
    :param rabbitmq_url: guest:guest@localhost:5672
    :param skill_name:
    :param skill_description:
    :param is_skill:
    :param parameters:
    :param version:
    :return:
    """
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def run(**dependencies: Any) -> None:
            client = RabbitMQClient(rabbitmq_url)
            await client.connect()

            # Регистрируем скилл в RabbitMQ
            await client.declare_queue("skills.registry")

            manifest = {
                "name": skill_name,
                "description": skill_description,
                "parameters": parameters,
                "request_queue": request_queue,
                "version": version,
                "is_skill": is_skill
            }

            await client.publish(
                queue="skills.registry",
                body=json.dumps(manifest, ensure_ascii=False),
            )

            logger.info(
                f"Скилл зарегистрирован: {skill_name} v{version}"
            )

            async def process_message(
                message: AbstractIncomingMessage,
            ) -> None:

                logger.info(
                    f"Получен запрос: queue={request_queue}, "
                    f"reply_to={message.reply_to}, "
                    f"correlation_id={message.correlation_id}, "
                    # f"body={message.body!r}"
                )

                headers = message.headers or {}

                missing = set(attributes) - headers.keys()
                if missing:
                    raise ValueError(f"Отсутствует обязательный аргумент в headers: {missing}")

                kwargs = {
                    name: headers[name]
                    for name in (*attributes, *optional_attributes)
                    if name in headers
                }
                # Дополнительные зависимости, например STT.
                overlap = kwargs.keys() & dependencies.keys()
                if overlap:
                    raise ValueError(f"Конфликтующие аргументы: {overlap}")

                result = await func(
                    message.body,
                    **kwargs,
                    **dependencies,
                )

                if result is None:
                    return

                if not isinstance(result, (str, bytes)):
                    raise TypeError("Worker must return str, bytes or None")

                response_headers = {
                    name: headers[name]
                    for name in attributes
                    if name in headers
                }

                logger.info(
                    f"Отправка ответа: queue={message.reply_to or response_queue}, "
                    f"correlation_id={message.correlation_id}"
                )

                await client.publish(
                    # queue=response_queue,
                    queue=message.reply_to or response_queue,
                    body=result,
                    headers=response_headers,
                    correlation_id=message.correlation_id,
                )

            try:
                logger.info(f"Worker запущен: {request_queue} -> {response_queue}")

                await client.consume(
                    request_queue,
                    process_message,
                    prefetch_count=1,
                )
            finally:
                await client.close()

        return run

    return decorator


if __name__ == "__main__":
    pass
