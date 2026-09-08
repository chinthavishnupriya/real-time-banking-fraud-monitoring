from kafka import KafkaConsumer, KafkaProducer
import json
import os
import requests
import time
from collections import defaultdict, deque


# --------------------------------------------------
# Datadog Configuration
# --------------------------------------------------

DD_API_KEY = os.getenv("DD_API_KEY")
DD_SITE = "datadoghq.com"

if not DD_API_KEY:
    raise RuntimeError("DD_API_KEY is not set")


# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------

consumer = KafkaConsumer(
    "bank-transactions",
    bootstrap_servers="localhost:9092",
    group_id="rapid-test",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


# --------------------------------------------------
# Kafka Producer for Fraud Alerts
# --------------------------------------------------

fraud_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# --------------------------------------------------
# Track Transactions Per Account
# --------------------------------------------------

account_transactions = defaultdict(deque)
account_locations = {}
WINDOW_SECONDS = 60
TRANSACTION_LIMIT = 5


# --------------------------------------------------
# Send Event to Datadog
# --------------------------------------------------

def send_to_datadog(transaction, risk="normal", alert_type=None):

    url = f"https://event-management-intake.{DD_SITE}/api/v2/events"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": DD_API_KEY
    }

    tags = [
        "application:banking",
        "environment:dev",
        "service:transaction-service",
        f"account:{transaction['account_id']}",
        f"merchant:{transaction['merchant']}",
        f"location:{transaction['location']}",
        f"status:{transaction['status']}"
    ]

    if risk == "high":
        tags.append("risk:high")

    if alert_type:
        tags.append(f"alert_type:{alert_type}")

    payload = {
        "data": {
            "type": "event",
            "attributes": {
                "category": "alert",
                "title": "Bank Transaction",
                "message": json.dumps(transaction),
                "tags": tags,
                "attributes": {
                    "status": "ok",
                    "priority": "5"
                }
            }
        }
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=10
    )

    print("Datadog:", response.status_code)


# --------------------------------------------------
# Publish Fraud Alert to Kafka
# --------------------------------------------------

def publish_fraud_alert(transaction, alert_type):

    fraud_alert = {
        "alert_type": alert_type,
        "risk": "high",
        "transaction": transaction
    }

    fraud_producer.send(
        "fraud-alerts",
        value=fraud_alert
    )

    fraud_producer.flush()

    print("🚨 Published to fraud-alerts:")
    print(fraud_alert)


# --------------------------------------------------
# Process Transactions
# --------------------------------------------------

print("Consumer started...")

for message in consumer:

    transaction = message.value

    account_id = transaction["account_id"]
    amount = transaction["amount"]

    current_time = time.time()
    location = transaction["location"]

    previous_location = account_locations.get(account_id)

    unusual_location = (
        previous_location is not None
        and previous_location != location
    )

    
    account_locations[account_id] = location
    # --------------------------------------------------
    # Store transaction timestamp for this account
    # --------------------------------------------------

    account_transactions[account_id].append(current_time)

    # Remove transactions older than 1 minute
    while (
        account_transactions[account_id]
        and current_time - account_transactions[account_id][0] > WINDOW_SECONDS
    ):
        account_transactions[account_id].popleft()


    # --------------------------------------------------
    # FRAUD RULE 1
    # High-value transaction > ₹50,000
    # --------------------------------------------------

    if amount > 50000:
        print("🚨 HIGH VALUE TRANSACTION:")
        print(transaction)
        publish_fraud_alert(transaction, "HIGH_VALUE_TRANSACTION")
        send_to_datadog(
            transaction,
            risk="high",
            alert_type="HIGH_VALUE_TRANSACTION"
        )

    elif unusual_location:
        print("🚨 UNUSUAL LOCATION FRAUD DETECTED!")
        print(
            f"Account {account_id} changed location "
            f"from {previous_location} to {location}."
        )
        publish_fraud_alert(transaction, "UNUSUAL_LOCATION")
        send_to_datadog(
            transaction,
            risk="high",
            alert_type="UNUSUAL_LOCATION"
        )

    else:
        print("Normal transaction:")
        print(transaction)
        send_to_datadog(transaction, risk="normal")


    # --------------------------------------------------
    # FRAUD RULE 2
    # 5 transactions from same account within 1 minute
    # --------------------------------------------------

    transaction_count = len(account_transactions[account_id])

    if transaction_count >= TRANSACTION_LIMIT:

        print("🚨 RAPID TRANSACTION FRAUD DETECTED!")
        print(
            f"Account {account_id} made "
            f"{transaction_count} transactions within 1 minute."
        )

        publish_fraud_alert(
            transaction,
            "RAPID_TRANSACTIONS"
        )

        send_to_datadog(
            transaction,
            risk="high",
            alert_type="RAPID_TRANSACTIONS"
        )

        # Clear the window so the same group does not
        # continuously trigger on every new transaction
        account_transactions[account_id].clear()