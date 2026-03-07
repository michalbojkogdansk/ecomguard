"""
EcomGuard - Synthetic Event Generator
======================================
Generates realistic e-commerce events for:
  - Customer registrations (contains PII -> GDPR scope)
  - Login attempts (includes suspicious patterns -> MTTD scope)
  - Orders (e-commerce transactions)
  - Clickstream events (user behaviour)
  - Support tickets (incident source)

Usage:
    pip install faker
    python generate_events.py

Output:
    JSON files written to ./output/ directory
    Ready to be ingested into Databricks / Kafka
"""

import json
import random
import os
from datetime import datetime, timedelta, timezone
from faker import Faker

fake = Faker("en_GB")  # UK locale for realistic European e-commerce / GDPR context
random.seed(42)        # Reproducible output

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configuration
NUM_CUSTOMERS      = 500
NUM_ORDERS         = 2000
NUM_LOGIN_EVENTS   = 3000
NUM_CLICK_EVENTS   = 5000
NUM_TICKETS        = 200

SUSPICIOUS_USER_IDS = [f"user_{i}" for i in random.sample(range(1, NUM_CUSTOMERS + 1), 10)]

PRODUCT_CATALOG = [
    {"product_id": "P001", "name": "Wireless Headphones", "price": 79.99, "category": "Electronics"},
    {"product_id": "P002", "name": "Running Shoes",       "price": 59.99, "category": "Sports"},
    {"product_id": "P003", "name": "Coffee Maker",        "price": 49.99, "category": "Kitchen"},
    {"product_id": "P004", "name": "Yoga Mat",            "price": 24.99, "category": "Sports"},
    {"product_id": "P005", "name": "Laptop Stand",        "price": 34.99, "category": "Electronics"},
    {"product_id": "P006", "name": "Water Bottle",        "price": 14.99, "category": "Sports"},
    {"product_id": "P007", "name": "Desk Lamp",           "price": 29.99, "category": "Home"},
    {"product_id": "P008", "name": "Protein Powder",      "price": 44.99, "category": "Health"},
]

def random_timestamp(days_back=90):
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (datetime.now(timezone.utc) - delta).isoformat() + "Z"

def write_jsonl(filename, records):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"  {len(records):?5} records -> {path}")

def generate_customers(n):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "user_id":        f"user_{i}",
            "event_type":     "customer_registered",
            "full_name":      fake.name(),
            "email":          fake.email(),
            "phone":          fake.phone_number(),
            "date_of_birth":  fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
            "address": {
                "street":   fake.street_address(),
                "city":     fake.city(),
                "postcode": fake.postcode(),
                "country":  "GB",
            },
            "registered_at":  random_timestamp(days_back=365),
            "marketing_opt_in": random.choice([True, False]),
            "tier":           random.choice(["bronze", "silver", "gold"]),
        })
    return customers

def generate_login_events(customers, n):
    events = []
    user_ids = [c["user_id"] for c in customers]
    for _ in range(n):
        user_id = random.choice(user_ids)
        is_suspicious = user_id in SUSPICIOUS_USER_IDS and random.random() < 0.3
        if is_suspicious:
            success = False
            country = random.choice(["RU", "CN", "KP", "IR"])
            ip      = fake.ipv4_public()
        else:
            success = random.random() > 0.05
            country = random.choice(["GB", "GB", "GB", "IE", "US"])
            ip      = fake.ipv4_public()
        events.append({
            "event_id":   fake.uuid4(),
            "event_type": "login_attempt",
            "user_id":    user_id,
            "timestamp":  random_timestamp(days_back=90),
            "success":    success,
            "ip_address": ip,
            "country":    country,
            "user_agent": fake.user_agent(),
            "suspicious": is_suspicious,
        })
    return events

def generate_orders(customers, n):
    orders = []
    user_ids = [c["user_id"] for c in customers]
    for _ in range(n):
        num_items = random.randint(1, 4)
        items = random.sample(PRODUCT_CATALOG, num_items)
        quantities = [random.randint(1, 3) for _ in items]
        total = round(sum(p["price"] * q for p, q in zip(items, quantities)), 2)
        orders.append({
            "order_id":   fake.uuid4(),
            "event_type": "order_placed",
            "user_id":    random.choice(user_ids),
            "timestamp":  random_timestamp(days_back=90),
            "status":     random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
            "items": [
                {
                    "product_id": p["product_id"],
                    "name":       p["name"],
                    "category":   p["category"],
                    "quantity":   q,
                    "unit_price": p["price"],
                }
                for p, q in zip(items, quantities)
            ],
            "total_gbp":  total,
            "payment_method": random.choice(["card", "paypal", "apple_pay"]),
            "shipping_address": {
                "street":   fake.street_address(),
                "city":     fake.city(),
                "postcode": fake.postcode(),
                "country":  "GB",
            },
        })
    return orders

def generate_clickstream(customers, n):
    pages = ["/", "/products", "/products/P001", "/products/P002",
             "/cart", "/checkout", "/account", "/orders", "/search"]
    events = []
    user_ids = [c["user_id"] for c in customers]
    for _ in range(n):
        events.append({
            "event_id":    fake.uuid4(),
            "event_type":  "page_view",
            "user_id":     random.choice(user_ids),
            "session_id":  fake.uuid4(),
            "timestamp":   random_timestamp(days_back=30),
            "page":        random.choice(pages),
            "referrer":    random.choice(["google", "direct", "email", "social", None]),
            "device":      random.choice(["desktop", "mobile", "tablet"]),
            "duration_ms": random.randint(500, 60000),
        })
    return events

def generate_support_tickets(customers, n):
    categories = ["payment_issue", "delivery_delay", "wrong_item", "refund_request", "account_access"]
    statuses   = ["open", "in_progress", "resolved", "closed"]
    tickets = []
    user_ids = [c["user_id"] for c in customers]
    for _ in range(n):
        created_at  = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 60))
        status      = random.choice(statuses)
        resolved_at = None
        if status in ["resolved", "closed"]:
            resolved_at = (created_at + timedelta(hours=random.randint(1, 72))).isoformat() + "Z"
        tickets.append({
            "ticket_id":   fake.uuid4(),
            "event_type":  "support_ticket",
            "user_id":     random.choice(user_ids),
            "category":    random.choice(categories),
            "status":      status,
            "priority":    random.choice(["low", "medium", "high", "critical"]),
            "created_at":  created_at.isoformat() + "Z",
            "resolved_at": resolved_at,
            "subject":     fake.sentence(nb_words=6),
        })
    return tickets

def main():
    print("\nEcomGuard Event Generator")
    print("=" * 40)
    print("\nGenerating synthetic data...")
    customers = generate_customers(NUM_CUSTOMERS)
    logins    = generate_login_events(customers, NUM_LOGIN_EVENTS)
    orders    = generate_orders(customers, NUM_ORDERS)
    clicks    = generate_clickstream(customers, NUM_CLICK_EVENTS)
    tickets   = generate_support_tickets(customers, NUM_TICKETS)
    print("\nWriting to output/...")
    write_jsonl("customers.jsonl",       customers)
    write_jsonl("login_events.jsonl",    logins)
    write_jsonl("orders.jsonl",          orders)
    write_jsonl("clickstream.jsonl",     clicks)
    write_jsonl("support_tickets.jsonl", tickets)
    suspicious_count = sum(1 for e in logins if e["suspicious"])
    print(f"\nDone!")
    print(f"  Suspicious login events: {suspicious_count} (ground truth for MTTD testing)")
    print(f"  All data is synthetic - no real PII")

if __name__ == "__main__":
    main()
