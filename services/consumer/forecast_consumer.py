from aiokafka import AIOKafkaConsumer
import os, json, asyncio, dotenv
from forecaster import SarimaxForecaster
import pandas as pd

# Load environment variables from .env file
dotenv.load_dotenv()
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")

def _deser(b):
    return json.loads(b.decode("utf-8"))

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=_deser,
        group_id="my-consumer-group",
        auto_offset_reset="earliest"  # start from oldest if no committed offset
    )
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            # TODO: Implement retraining for SarimaxForecaster

            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())