from kafka import KafkaProducer
import json
import time
import uuid

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

locations = ["Hyderabad", "Hyderabad", "Mumbai"]

for location in locations:
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": "ACC888",
        "amount": 10000,
        "currency": "INR",
        "merchant": "Amazon",
        "location": location,
        "status": "SUCCESS"
    }

    producer.send("bank-transactions", value=transaction)

    print("Location test:", transaction)

    time.sleep(2)

producer.flush()

print("Location test completed.")