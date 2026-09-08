# Real-Time Banking Fraud Monitoring System

<p align="center">
  <strong>Real-time transaction streaming • Fraud detection • Apache Kafka • Datadog</strong>
</p>

## Overview
A real-time banking transaction monitoring system that generates transaction events, streams them through Apache Kafka, processes them using two consumer instances, detects potentially fraudulent activity, publishes fraud alerts to a dedicated Kafka topic, and sends monitoring events to Datadog.

## Fraud Detection Rules
| Rule | Detection condition | Action |
|---|---|---|
| 🔴 High-value | Amount > ₹50,000 | High-risk event + fraud alert |
| 🟠 Rapid transactions | 5 transactions from the same account within 1 minute | High-risk event + fraud alert |
| 🟡 Unusual location | Account appears from a different observed location | High-risk event + fraud alert |

## Kafka Configuration
| Topic | Partitions | Purpose |
|---|---:|---|
| `bank-transactions` | **3** | Incoming banking transactions |
| `fraud-alerts` | **3** | Detected fraud events |

Two consumer instances run in the same consumer group and share the three partitions.

## Datadog Monitoring
Datadog is used for transaction events, high-risk fraud events, Kafka consumer lag, Kafka broker offsets, dashboard visualization, and high-value fraud alerting.

**High-risk event query:**
```text
application:banking AND risk:high
```

## Performance
| Test | Result |
|---|---:|
| Transactions generated | **100** |
| Test duration | **60.01 seconds** |
| Measured throughput | **99.98 transactions/minute** |

## Project Evidence
The implementation was tested across Kafka, Python consumers, fraud detection, Datadog Events, dashboard monitoring, and alerting.

| # | Evidence |
|---:|---|
| 01 | Kafka `bank-transactions` topic — 3 partitions |
| 02 | Kafka `fraud-alerts` topic — 3 partitions |
| 03 | Two active Kafka consumers and partition assignment |
| 04 | High-value and unusual-location fraud detection |
| 05 | Rapid-transaction fraud detection |
| 06 | 100 transactions/minute throughput test |
| 07 | Datadog banking transaction events |
| 08 | Datadog high-risk events |
| 09 | Kafka consumer monitoring |
| 10 | Datadog dashboard |
| 11 | Datadog fraud monitor configuration |

Individual evidence screenshots are stored in the `screenshots/` directory.

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
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

## Security
Datadog API credentials are supplied through environment variables and are excluded from version control. No API key is stored in the repository.

## Outcome
**Transaction generation → Kafka streaming → multi-consumer processing → fraud detection → fraud-event publishing → Datadog observability → dashboard visualization → high-risk alerting**
