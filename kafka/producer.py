
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ["Delhi", "Mumbai", "Bangalore"]

USE_SAMPLE_FILE = True   

if USE_SAMPLE_FILE:
    with open("data/sample_trips.json") as f:
        for line in f:
            producer.send("uber_trips", json.loads(line))
            print("Sent:", line.strip())
            time.sleep(1)
else:
    while True:
        trip = {
            "trip_id": random.randint(1000, 9999),
            "city": random.choice(cities),
            "fare": random.randint(100, 1000),
            "status": random.choice(["completed", "cancelled"])
        }
        producer.send("uber_trips", trip)
        print("Sent:", trip)
        time.sleep(2)
