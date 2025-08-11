import os, json, asyncio
import dotenv
from datetime import datetime
from aiokafka import AIOKafkaProducer
import httpx

# Load environment variables from .env file
dotenv.load_dotenv()

SENSOR_ID = 5077866  # Example sensor ID
API = f"https://api.openaq.org/v3/sensors/{SENSOR_ID}/measurements"
OPENAQ_KEY = os.getenv("OPENAQ_API_KEY", None)
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")


def _ser(v): 
    return json.dumps(v, ensure_ascii=False).encode("utf-8")

def _utc(dt: datetime):
    return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

async def fetch_hourly_data(client: httpx.AsyncClient):
    # fetch data from OpenAQ API
    r = await client.get(API)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    return results

def normalize(data: dict) -> dict:
    # Normalize data if needed
    time_obj = data.get("period").get("datetimeFrom")
    ts = time_obj.get("utc") or time_obj.get("local")
    return {
        "sensor_id": 5077866,
        "value": data.get("value"),
        "ts": ts,
        "parameter": data.get("parameter").get("name"),
        "value": data.get("value"),
        "source": "openaq_v3",
    }
    

async def main():
    if not OPENAQ_KEY:
        raise ValueError("OPENAQ_API_KEY environment variable not set")
    
    headers = {
        "X-API-KEY": OPENAQ_KEY
    }

    async with httpx.AsyncClient(headers=headers) as client:
        rows = await fetch_hourly_data(client)
        if not rows:
            print("No data fetched from OpenAQ")
            return
        # produce to kafka
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=_ser,
            acks= 'all',
            enable_idempotence=True,
            linger_ms=10,
            compression_type='gzip', # gzip fine but figure out zstd
        )
        await producer.start()
        try:
            sent = 0
            for m in rows:
                m = normalize(m)
                if not m.get("ts"):
                    continue
                key = str(m["sensor_id"]).encode("utf-8")
                await producer.send_and_wait(TOPIC, value=m, key=key)
                sent += 1
            print("producer sent {} messages".format(sent))
        finally:
            await producer.stop()

# demo it:
if __name__ == "__main__":
    # rows = [
    #     {"sensor_id": 1, "value": 42, "timestamp": "2023-10-01T12:00:00Z"},
    #     {"sensor_id": 2, "value": 36, "timestamp": "2023-10-01T12:01:00Z"},
    # ]
    asyncio.run(main())
