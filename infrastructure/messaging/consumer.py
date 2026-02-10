import json
import os

import pika

AMQP_URL = os.environ["AMQP_URL"]

params = pika.URLParameters(AMQP_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(
    exchange="dlx",
    exchange_type="direct",
    durable=True,
)

channel.queue_declare(
    queue="task_queue.dlq",
    durable=True,
)

channel.queue_bind(
    exchange="dlx",
    queue="task_queue.dlq",
    routing_key="task_queue",
)

channel.exchange_declare(
    exchange='events',
    exchange_type='topic',
    durable=True)

channel.queue_declare(
    queue="task_queue",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "dlx",
        "x-dead-letter-routing-key": "task_queue",
    }
)

channel.queue_bind(
    exchange='events',
    queue='task_queue',
    routing_key='task.*'
)

def handle_message(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f'Processing: {data}')

        if data['task_id'] == 1:
            raise RuntimeError('DB is down')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f'Error: {e}')

        ch.basic_nack(delivery_tag=method.delivery_tag,
                      requeue=False
        )

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=handle_message)

print(' [x] Awaiting for messages')
channel.start_consuming()
