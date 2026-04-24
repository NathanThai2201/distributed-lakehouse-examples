import time
import json
import random
import pandas as pd
from confluent_kafka import Producer #type: ignore

# Kafka config
conf = {
    'bootstrap.servers': 'localhost:9093'  # change to 9093 if using second broker
}

producer = Producer(conf)

# Load parquet
df = pd.read_parquet("yellow_taxi_mock.parquet")

print(f"Loaded {len(df)} rows")

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    # else:
    #     print(f"Sent to {msg.topic()} [{msg.partition()}]")

while True:
    # Pick random row
    row = df.sample(1).to_dict(orient="records")[0]

    # Convert to JSON
    message = json.dumps(row, default=str)

    # Send to Kafka
    producer.produce(
        topic="quickstart-events",
        value=message,
        callback=delivery_report
    )

    producer.poll(0)

    print(f"Sent: {message[:100]}...")

    # Control speed (adjust as needed)
    time.sleep(random.uniform(0.5, 2))