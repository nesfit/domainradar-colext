from aiokafka import AIOKafkaProducer
import asyncio

async def run():
     producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
     await producer.start()
     import random
     ip = [random.randint(1, 255), random.randint(1, 255), random.randint(1, 255), random.randint(1, 255)]
     for i in range(200):
             ip = [random.randint(1, 255), random.randint(1, 255), random.randint(1, 255), random.randint(1, 255)]
             ips = f"{ip[0]}.{ip[1]}.{ip[2]}.{ip[3]}"
             key = '{"first":"xyz.cz","second":"' + ips + '"}'
             await producer.send_and_wait("to_process_IP", key=key.encode(), value=None)
     await producer.stop()

asyncio.run(run())