import json
import os

import pika

AMQP_URL = os.environ['AMQP_URL']

params = pika.URLParameters(AMQP_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the general queue
channel.exchange_declare(
    exchange='events',
    exchange_type='topic',
    durable=True)

channel.queue_declare(
    queue='task_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'dlx',
        'x-dead-letter-routing-key': 'task_queue',
    }
)

channel.queue_bind(
    exchange='events',
    queue='task_queue',
    routing_key='task.*'
)

# Declare the DLX queue
channel.exchange_declare(
    exchange='dlx',
    exchange_type='direct',
    durable=True,
)

channel.queue_declare(
    queue='task_queue.dlq',
    durable=True,
)

channel.queue_bind(
    exchange='dlx',
    queue='task_queue.dlq',
    routing_key='task_queue',
)

# Declare retry
channel.exchange_declare(
    exchange='retry',
    exchange_type='direct',
    durable=True,
)

channel.queue_declare(
    queue='task_queue.retry',
    durable=True,
    arguments={
        'x-message-ttl': 5000,  # 5 sec
        'x-dead-letter-exchange': 'events',
        'x-dead-letter-routing-key': 'task.created',
    },
)

channel.queue_bind(
    exchange='retry',
    queue='task_queue.retry',
    routing_key='task_queue',
)


def handle_message(ch, method, properties, body):
    headers = properties.headers or {}
    retries = headers.get('x-retry', 0)
    try:
        data = json.loads(body)
        print(f'Processing: {data}, attempts: {retries + 1}')

        if data['task_id'] == 1:
            raise RuntimeError('DB is down')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        if retries < 2:
            print('Retrying...')
            
            new_headers = headers | {'x-retry': retries + 1}
            
            ch.basic_publish(
                exchange='retry',
                routing_key='task_queue',
                body=body,
                properties=pika.BasicProperties(
                    headers=new_headers,
                    delivery_mode=2,
                )
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print('Sending to DLQ')

            ch.basic_nack(delivery_tag=method.delivery_tag,
                          requeue=False
            )

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=handle_message)

print(' [x] Awaiting for messages')
channel.start_consuming()
