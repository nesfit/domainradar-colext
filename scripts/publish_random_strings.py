#!/usr/bin/env python3

from aiokafka import AIOKafkaProducer
import asyncio
from asyncio.exceptions import CancelledError
import random
import string

def rnd_str(len):
    return ''.join(random.choices(string.ascii_letters, k=10))

async def run():
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()

    try:
        while True:
            await producer.send_and_wait("test", key=rnd_str(10).encode(), value=rnd_str(5).encode())
            await asyncio.sleep(0.01)
    except (KeyboardInterrupt, CancelledError) as e:
        print('slut')

    await producer.stop()

asyncio.run(run())
