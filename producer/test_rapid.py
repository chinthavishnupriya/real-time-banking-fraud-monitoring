from kafka import KafkaProducer
import json
import time
import uuid

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(5):

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": "ACC999",
        "amount": 10000,
        "currency": "INR",
        "merchant": "Amazon",
        "location": "Hyderabad",
        "status": "SUCCESS"
    }

    producer.send(
        "bank-transactions",
        value=transaction
    )

    print("Test transaction:", transaction)

    time.sleep(2)

producer.flush()
print("5 test transactions sent.")