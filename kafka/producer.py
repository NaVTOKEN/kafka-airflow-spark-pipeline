
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ["Delhi", "Mumbai", "Bangalore"]

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
