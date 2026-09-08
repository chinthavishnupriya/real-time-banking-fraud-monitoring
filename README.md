# Real-Time Banking Fraud Monitoring System

<p align="center">
  <strong>Real-time transaction streaming • Fraud detection • Kafka • Datadog</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Kafka-Streaming-black?logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Docker-Containerized-blue?logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/Datadog-Monitoring-purple?logo=datadog&logoColor=white" alt="Datadog">
</p>

## Overview

A real-time banking transaction monitoring system that generates transaction events, streams them through Apache Kafka, processes them using two consumer instances, detects potentially fraudulent activity, publishes fraud alerts to a dedicated Kafka topic, and sends monitoring events to Datadog.

## Architecture

```text
                    +----------------------+
                    |  Transaction Producer |
                    +----------+-----------+
                               |
                               v
                 +----------------------------+
                 | Kafka: bank-transactions   |
                 |        3 partitions        |
                 +-------------+--------------+
                               |
                     +---------+---------+
                     |                   |
                     v                   v
              +-----------+       +-----------+
              | Consumer 1|       | Consumer 2|
              +-----+-----+       +-----+-----+
                    \                   /
                     \                 /
                      +-------+-------+
                              |
                              v
                    +-------------------+
                    | Fraud Detection   |
                    +---------+---------+
                              |
              +---------------+---------------+
              |               |               |
              v               v               v
        High Value       Rapid Transactions  Unusual Location
              |               |               |
              +---------------+---------------+
                              |
                 +------------+------------+
                 |                         |
                 v                         v
       Kafka: fraud-alerts          Datadog Events
                                      |
                                      v
                              Dashboard + Monitor
                                      |
                                      v
                                    Alert
```

## Fraud Detection Rules

| Rule | Condition | Result |
|---|---|---|
| **High-value transaction** | Amount > ₹50,000 | High-risk event |
| **Rapid transactions** | 5 transactions from the same account within 1 minute | High-risk event |
| **Unusual location** | Account transaction appears from a different observed location | High-risk event |

## Kafka Configuration

| Topic | Partitions | Purpose |
|---|---:|---|
| `bank-transactions` | 3 | Incoming banking transactions |
| `fraud-alerts` | 3 | Detected fraud events |

Two consumer instances run in the same consumer group and share the three partitions.

## Datadog Monitoring

The project sends transaction and fraud events to Datadog and provides monitoring for:

- Banking transaction events
- High-risk fraud events
- Kafka consumer lag
- Kafka broker offsets
- Dashboard visualization
- High-value fraud alerting

### High-Risk Query

```text
application:banking AND risk:high
```

## Performance Result

The controlled throughput test achieved:

| Metric | Result |
|---|---:|
| Transactions sent | **100** |
| Elapsed time | **60.01 seconds** |
| Measured rate | **99.98 transactions/minute** |

## Evidence

The repository includes a consolidated evidence image covering the major implementation and monitoring stages.

<p align="center">
  <img src="screenshots/project-evidence.jpg" alt="Project implementation and monitoring evidence" width="100%">
</p>

### Evidence includes

1. Kafka `bank-transactions` topic with 3 partitions
2. Kafka `fraud-alerts` topic with 3 partitions
3. Two active Kafka consumers and partition assignment
4. High-value and unusual-location fraud detection
5. Rapid-transaction fraud detection
6. 100 transactions/minute throughput test
7. Datadog banking transaction events
8. Datadog high-risk events
9. Kafka consumer monitoring
10. Datadog dashboard
11. Datadog monitor configuration

## Project Structure

```text
real-time-banking-fraud-monitoring/
├── consumer/
│   └── consumer.py
├── producer/
│   ├── producer.py
│   ├── test_rapid.py
│   ├── test_location.py
│   └── test_100_per_min.py
├── docs/
├── screenshots/
│   └── project-evidence.jpg
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

## Security

Datadog API credentials are supplied through environment variables and are excluded from version control. No API key is stored in this repository.

## Outcome

The project demonstrates a complete real-time monitoring pipeline: transaction generation → Kafka streaming → multi-consumer processing → fraud detection → fraud-event publishing → Datadog observability → dashboard visualization → high-risk alerting.
