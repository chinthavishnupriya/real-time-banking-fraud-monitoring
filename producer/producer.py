from kafka import KafkaProducer
import json
import random
import time
import uuid

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

merchants = [
    "Amazon",
    "Flipkart",
    "ABC Jewellers",
    "BigBasket",
    "Myntra"
]

locations = [
    "Hyderabad",
    "Bangalore",
    "Mumbai",
    "Delhi"
]

while True:
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(100, 999)}",
        "amount": random.randint(100, 100000),
        "currency": "INR",
        "merchant": random.choice(merchants),
        "location": random.choice(locations),
        "status": "SUCCESS"
    }

    producer.send(
        "bank-transactions",
        key=transaction["account_id"].encode("utf-8"),
        value=transaction
    )

    print("Produced:", transaction)

    time.sleep(0.6)