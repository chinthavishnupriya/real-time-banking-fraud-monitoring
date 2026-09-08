from kafka import KafkaProducer
import json
import random
import time
import uuid

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

merchants = ["Amazon", "Flipkart", "ABC Jewellers", "BigBasket", "Myntra"]
locations = ["Hyderabad", "Bangalore", "Mumbai", "Delhi"]

interval = 60 / 99

start = time.time()

for i in range(100):
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
        value=transaction
    )

    print(f"{i + 1}/100 transaction sent")

    if i < 99:
        target_time = start + (i + 1) * interval
        sleep_time = target_time - time.time()

        if sleep_time > 0:
            time.sleep(sleep_time)

elapsed = time.time() - start

producer.flush()

print("\n==============================")
print("THROUGHPUT TEST COMPLETED")
print("==============================")
print(f"Transactions sent : 100")
print(f"Elapsed time      : {elapsed:.2f} seconds")
print(f"Rate              : {100 / elapsed * 60:.2f} transactions/minute")