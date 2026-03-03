import asyncio
import json
import os
import signal
import aio_pika

AMQP_URL = os.environ['AMQP_URL']

class Worker:
    def __init__(self):
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.AbstractChannel | None = None
        self.exchange: aio_pika.AbstractExchange | None = None
        self.queue: aio_pika.AbstractQueue | None = None
        self.should_stop: asyncio.Event()

    async def connect(self):
        self.connection = await aio_pika.connect_robust(AMQP_URL)
        self.channel = await self.connection.channel()

        await self.channel.set_qos(prefetch_count=1)

        self.exchange = await self.channel.declare_exchange(
            'events',
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        dlx = await self.channel.declare_exchange(
            'dlx',
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        retry_exchange = await self.channel.declare_exchange(
            'retry',
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        dlq = await self.channel.declare_exchange(
            'task_queue.dlq',
            durable=True,
        )

        await dlq.bind(dlx, routing_key='task_queue')

        retry_queue = await self.channel.declare_queue(
            'task_queue.retry',
            durable=True,
            arguments={
                'x-message-ttl': 5000,
                'x-dead-letter-exchange': 'events',
                'x-dead-letter-routing-key': 'task.created',
            }
        )

        await retry_queue.bind(retry_exchange, routing_key='task.queue')

        # The main queue
        self.queue = await self.channel.declare_queue(
            'task_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': 'task_queue',
            },
        )

        await self.queue.bind(self.exchange, routing_key='task.*')

    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process(ignore_processed=True):
            headers = message.headers or {}
            retries = headers.get('x-retries', 0)

            data = json.loads(message.body.decode())
            print(f'Processing {data}, attempt {retries + 1}')

            try:
                if data['task_id'] == 1:
                    raise RuntimeError('Temporary DB error')

                print('Success')

            except Exception as e:
                print(f'Error: ', e)

                if retries < 2:
                    print('Retrying')

                    new_headers = {**headers, "x-retries": retries + 1}

                    retry_exchange = await self.channel.get_exchange("retry")

                    await retry_exchange.publish(
                        aio_pika.Message(
                            body=message.body,
                            headers=new_headers,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key="task_queue",
                    )
                print("Sending to DLQ")
                await message.reject(requeue=False)

async def main():
    worker = Worker()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(worker.shutdown())
        )

    await worker.start()

if __name__ == '__main__':
    asyncio.run(main())
