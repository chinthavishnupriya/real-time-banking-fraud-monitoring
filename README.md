# Real-Time Banking Fraud Monitoring System

A real-time banking transaction monitoring system built with **Python, Apache Kafka, Docker, and Datadog**.

## Overview

The system generates banking transactions, streams them through Kafka, processes them using two consumer instances, detects potentially fraudulent activity, publishes fraud events to a dedicated Kafka topic, and sends events to Datadog for observability and alerting.

## Architecture

```text
Transaction Producer
        |
        v
Kafka: bank-transactions (3 partitions)
        |
        +-------------------+
        |                   |
   Consumer 1          Consumer 2
        |                   |
        +---------+---------+
                  |
          Fraud Detection
          /      |       \
         /       |        \
High Value   Rapid Txns   Unusual Location
         \       |        /
          \      |       /
        +---------+---------+
        |                   |
        v                   v
Kafka: fraud-alerts     Datadog Events
                            |
                            v
                    Dashboard & Monitor
                            |
                            v
                          Alert
```

## Fraud Detection Rules

1. **High-value transaction:** amount greater than ₹50,000.
2. **Rapid transactions:** five transactions from the same account within one minute.
3. **Unusual location:** a transaction from a different location than the account's previously observed location.

## Kafka

- `bank-transactions`: 3 partitions; receives banking transactions.
- `fraud-alerts`: 3 partitions; receives detected fraud events.
- Two consumer instances process `bank-transactions` as a consumer group.

## Datadog

Datadog is used for transaction events, high-risk events, Kafka consumer lag, Kafka broker offsets, dashboards, and high-value fraud alerting.

High-risk events are filtered with:

```text
application:banking AND risk:high
```

## Performance

A controlled throughput test produced **100 transactions in 60.01 seconds**, measuring **99.98 transactions/minute**.

## Evidence / Screenshots

### 1. Kafka Topic & Partition Configuration

![Kafka bank-transactions topic](screenshots/01-kafka-bank-transactions.png)

### 2. Fraud Alert Kafka Topic Configuration

![Kafka fraud-alerts topic](screenshots/02-kafka-fraud-alerts.png)

### 3. Two Kafka Consumer Instances & Partition Assignment

![Two Kafka consumers](screenshots/03-two-consumers.png)

### 4. Fraud Detection and Datadog Event Publishing

![Fraud detection and Datadog publishing](screenshots/04-fraud-detection-datadog.png)

### 5. Rapid Transaction Fraud Detection

![Rapid transaction detection](screenshots/05-rapid-transactions.png)

### 6. 100 Transactions/Minute Throughput Test

![Throughput test](screenshots/06-throughput-test.png)

### 7. Datadog Event Explorer: Banking Transaction Events

![Datadog transaction events](screenshots/07-datadog-events.png)

### 8. Datadog High-Risk Event Explorer

![Datadog high risk events](screenshots/08-datadog-high-risk.png)

### 9. Datadog Kafka Consumer Monitoring

![Datadog Kafka consumer monitoring](screenshots/09-kafka-monitoring.png)

### 10. Datadog Dashboard

![Datadog dashboard](screenshots/10-datadog-dashboard.png)

### 11. Datadog High-Value Fraud Monitor Alert

![Datadog monitor alert](screenshots/11-datadog-monitor-alert.png)

## Result

The project demonstrates real-time transaction streaming, three-partition Kafka topics, two consumer instances, three fraud-detection rules, fraud-event publishing, Datadog observability, dashboard visualization, and high-risk alerting.

## Security Note

Datadog API keys are supplied through environment variables and are not stored in the repository.
