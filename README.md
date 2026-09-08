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

The project evidence screenshots are collected in the image below. They cover Kafka topic configuration, consumer processing, fraud detection, throughput testing, Datadog events, Kafka monitoring, dashboard visualization, and monitor configuration.

![Project evidence screenshots](screenshots/project-evidence.jpg)

## Result

The project demonstrates real-time transaction streaming, three-partition Kafka topics, two consumer instances, three fraud-detection rules, fraud-event publishing, Datadog observability, dashboard visualization, and high-risk alerting.

## Security Note

Datadog API keys are supplied through environment variables and are not stored in the repository.
