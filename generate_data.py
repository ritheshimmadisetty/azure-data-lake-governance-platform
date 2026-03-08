import pandas as pd
import random
from datetime import datetime, timedelta

# Generate customers
customers = []
for i in range(1, 201):
    customers.append({
        "customer_id": i,
        "customer_name": f"Customer_{i}",
        "city": random.choice(["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai"]),
        "segment": random.choice(["Retail", "Wholesale", "Online"]),
        "created_date": "2022-01-01"
    })

# Generate products
products = []
for i in range(1, 51):
    products.append({
        "product_id": i,
        "product_name": f"Product_{i}",
        "category": random.choice(["Electronics", "Clothing", "Grocery", "Furniture"]),
        "unit_price": round(random.uniform(100, 5000), 2)
    })

# Generate sales orders
orders = []
start_date = datetime(2023, 1, 1)
for i in range(1, 1001):
    order_date = start_date + timedelta(days=random.randint(0, 500))
    orders.append({
        "order_id": i,
        "customer_id": random.randint(1, 200),
        "product_id": random.randint(1, 50),
        "quantity": random.randint(1, 10),
        "order_date": order_date.strftime("%Y-%m-%d"),
        "status": random.choice(["Completed", "Cancelled", "Pending"])
    })

pd.DataFrame(customers).to_csv("customers.csv", index=False)
pd.DataFrame(products).to_csv("products.csv", index=False)
pd.DataFrame(orders).to_csv("orders.csv", index=False)

print("Files created: customers.csv, products.csv, orders.csv")