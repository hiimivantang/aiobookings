from aiokafka import AIOKafkaConsumer
import asyncio
import random
import json
import sys


async def no_sleep(previous_attempt_number, delay_since_first_attempt_ms):
    """Don't sleep at all before retrying."""
    return 0 
async def fixed_sleep(seconds=1):
    """Sleep a fixed amount of time between each retry."""
    await asyncio.sleep(seconds)

async def random_sleep(random_min=1, random_max=2):
    """Sleep a random amount of time between wait_random_min and wait_random_max"""
    await asyncio.sleep(random.randint(random_min, random_max))

async def run(cmd, retries=None):
    #async with sem:
    retry_counter = 0
    while True:
        if retries:
            if retry_counter > retries:
                break
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)

            stdout, stderr = await proc.communicate()
            if stdout:
                break
            if stderr:
                retry_counter += 1
                raise Exception(f'[stderr]\n{stderr.decode()}')
            break
        except Exception as e:
            print(e)
            await random_sleep(1,5)

async def post_to_external_system(topic, msg, retries=None):
    cmd = f"docker exec -it {topic}_EXTERNAL_SYSTEM /bin/ash -c \"echo {msg} >> /{topic}_EXTERNAL_SYSTEM/bookings.log\""
    await run(cmd)

async def consume(topic):
    subtasks = []
    consumer = AIOKafkaConsumer(topic, loop=loop, bootstrap_servers='localhost:9092',group_id="SIMPLE_SERVICE", enable_auto_commit=False)
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            #print("consumed: ", msg.topic, msg.partition, msg.offset,msg.value, msg.timestamp)
            await post_to_external_system(msg.topic, str(msg.value).replace('"', r'\"'))
            await consumer.commit()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

if __name__ == '__main__':
    
    #TODO: Autodiscovery of new hotels
    topics = ['HOTEL_1','HOTEL_2','HOTEL_3']#,'HOTEL_4','HOTEL_5','HOTEL_6','HOTEL_7','HOTEL_8',]
    subtasks = []
    for topic in topics: 
        subtasks.append(consume(topic))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*subtasks))

