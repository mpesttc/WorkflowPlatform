import json
import os
import time

import pika

AMQP_URL = os.environ.get("AMQP_URL")

params = pika.URLParameters(AMQP_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='events', exchange_type='topic', durable=True)

event = {
    "event": "task.created",
    "task_id": 1,
}

channel.basic_publish(
    exchange='events',
    routing_key='task.created',
    body=json.dumps(event),
    properties=pika.BasicProperties(delivery_mode=2),
)

print(f"{time.ctime()}: Event published")

connection.close()


