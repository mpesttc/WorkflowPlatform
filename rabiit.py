import os
import aio_pika

AMQP_URL = os.environ['AMQP_URL']

connection: aio_pika.RobustConnection | None = None
channel: aio_pika.AbstractChannel | None = None
exchange: aio_pika.AbstractExchange | None = None


async def init_rabbit():
    global connection, channel, exchange

    connection = await aio_pika.connect_robust(url=AMQP_URL)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        'events',
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

def get_exchange():
    global exchange
    return exchange

async def close_rabbit():
    global connection
    if connection:
        await connection.close()
