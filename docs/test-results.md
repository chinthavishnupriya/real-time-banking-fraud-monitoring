# Test Results

## 1. Throughput Test

- Transactions sent: 100
- Elapsed time: 60.01 seconds
- Measured rate: 99.98 transactions/minute
- Status: PASS

## 2. High-Value Transaction Detection

Condition:
- Transaction amount > ?50,000

Result:
- High-value transaction detected
- Fraud event published to raud-alerts
- Datadog event accepted with HTTP 202
- Status: PASS

## 3. Rapid Transaction Detection

Condition:
- 5 transactions from the same account within 1 minute

Result:
- Rapid transaction fraud detected
- Fraud event published to raud-alerts
- Datadog event accepted with HTTP 202
- Status: PASS

## 4. Unusual Location Detection

Condition:
- Account appears from a different observed location

Result:
- Unusual location fraud detected
- Fraud event published to raud-alerts
- Datadog event accepted with HTTP 202
- Status: PASS

## 5. Kafka Consumer Test

- Consumer instances: 2
- Kafka topic: ank-transactions
- Partitions: 3
- Consumer lag during verification: 0
- Status: PASS

## Overall Result

The real-time banking fraud monitoring system successfully demonstrated:

Transaction generation ? Kafka streaming ? multi-consumer processing ? fraud detection ? fraud-alert publishing ? Datadog monitoring ? dashboard visualization ? high-risk alerting.
