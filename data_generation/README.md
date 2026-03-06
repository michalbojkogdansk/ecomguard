# Data Generation

Synthetic e-commerce event factory built with [Faker](https://faker.readthedocs.io/).

## What it generates

| File | Records | Description |
|---|---|---|
| `customers.jsonl` | 500 | Customer registrations - contains PII (GDPR scope) |
| `login_events.jsonl` | 3,000 | Login attempts - includes suspicious brute-force patterns |
| `orders.jsonl` | 2,000 | E-commerce orders with line items |
| `clickstream.jsonl` | 5,000 | Page view / navigation events |
| `support_tickets.jsonl` | 200 | Support tickets - source of MTTR data |

## Why JSONL?

Newline-delimited JSON is the standard format for:
- Kafka event streaming (one message per line)
- Databricks ingestion (spark.read.json handles JSONL natively)
- Append-only event logs (each line = one event)

## GDPR-relevant fields

The following fields are PII and will be masked in the compliance pipeline:

| Dataset | PII Fields |
|---|---|
| customers | full_name, email, phone, date_of_birth, address |
| login_events | ip_address |
| orders | shipping_address |

## Running locally

```bash
pip install faker
python generate_events.py
```

## Suspicious patterns (MTTD)

10 user IDs are designated as suspicious. Their login events include:
- Failed login attempts
- Logins from unusual countries (RU, CN, KP, IR)

The `suspicious: true` field is the ground truth label for testing detection logic.
