from contextlib import asynccontextmanager

from fastapi import FastAPI
import json
import aio_pika

from rabiit import init_rabbit, close_rabbit, get_exchange


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_rabbit()
    yield

    # Shutdown
    await close_rabbit()

app = FastAPI(lifespan=lifespan)

@app.post('/tasks')
async def create_task(task_id: int):
    event = {
        'event': 'task.created',
        'task_id': task_id,
    }

    exchange = get_exchange()

    message = aio_pika.Message(
        body=json.dumps(event).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await exchange.publish(message, routing_key='task.created')

    return {"status": "queued"}
