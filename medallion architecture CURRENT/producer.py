import time
import json
import random
import pandas as pd
from confluent_kafka import Producer #type: ignore
from datetime import datetime, timedelta

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
    row = df.sample(1).to_dict(orient="records")[0]

    # Generate timestamps
    dropoff = datetime.now()
    pickup = dropoff - timedelta(minutes=random.randint(0, 60))

    # Format to Spark-style timestamp string
    row["tpep_dropoff_datetime"] = dropoff.strftime("%Y-%m-%d %H:%M:%S")
    row["tpep_pickup_datetime"] = pickup.strftime("%Y-%m-%d %H:%M:%S")

    message = json.dumps(row, default=str)

    producer.produce(
        topic="yellow-tripdata",
        value=message,
        callback=delivery_report
    )

    producer.poll(0)

    print(f"Sent: {message[:100]}...")

    time.sleep(random.uniform(0.5, 2))