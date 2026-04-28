#!/usr/bin/env python3
"""Script to generate sample_input.csv — run once during setup."""
import csv
import random
import datetime

random.seed(42)

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home", "Beauty", "Toys"]
STATUSES = ["completed", "pending", "cancelled", "refunded"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]

rows = []
start_date = datetime.date(2023, 1, 1)

for i in range(1, 2201):
    sale_date = start_date + datetime.timedelta(days=random.randint(0, 364))
    category = random.choice(CATEGORIES)
    quantity = random.randint(1, 20)
    unit_price = round(random.uniform(5.0, 999.99), 2)
    discount = round(random.uniform(0, 0.30), 2)
    total = round(quantity * unit_price * (1 - discount), 2)

    # Sprinkle some nulls and dirty data intentionally so validation catches them
    customer_age = random.randint(18, 75) if random.random() > 0.03 else None
    email = f"user{i}@example.com" if random.random() > 0.02 else None
    rating = round(random.uniform(1.0, 5.0), 1) if random.random() > 0.04 else None
    # Occasionally insert an out-of-range value to test range checks
    if random.random() < 0.01:
        unit_price = -5.0
    if random.random() < 0.01:
        quantity = 0

    rows.append({
        "order_id": f"ORD-{i:05d}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_age": customer_age,
        "email": email,
        "region": random.choice(REGIONS),
        "category": category,
        "product_name": f"{category} Item {random.randint(1, 500)}",
        "quantity": quantity,
        "unit_price": unit_price,
        "discount": discount,
        "total_amount": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
        "rating": rating,
        "sale_date": sale_date.isoformat(),
    })

# Add ~50 duplicate rows to test deduplication
for _ in range(50):
    rows.append(random.choice(rows[:500]))

random.shuffle(rows)

with open("data/sample_input.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} rows in data/sample_input.csv")
