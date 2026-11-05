# Jorah Broker

Shared asynchronous RabbitMQ client for Jorah services.

## Installation

```bash
pip install git+https://github.com/YOUR_USERNAME/jorah-broker.git@v0.1.0
```

## Usage

```python
from jorah_broker import RabbitMQClient

rabbitmq = RabbitMQClient()

await rabbitmq.connect()
```

## Configuration

Set the `RABBITMQ_URL` environment variable:

```env
RABBITMQ_URL=amqp://user:password@localhost:5672/
```

## Requirements

- Python >= 3.12
- RabbitMQ
- aio-pika