import pandas as pd
import sqlite3
import random
from faker import Faker
from pathlib import Path
from datetime import datetime, timedelta

# Initialize
fake = Faker()
num_rows = 5000
Path("data").mkdir(parents=True, exist_ok=True)

# -------- Create Large Parquet Dataset --------
def generate_parquet_data(n):
    ride_ids = range(1000, 1000 + n)
    customer_ids = [f"C{random.randint(100, 999)}" for _ in range(n)]
    amounts = [round(random.uniform(100, 2000), 2) for _ in range(n)]
    timestamps = [datetime(2024, 11, 1) + timedelta(minutes=random.randint(0, 100000)) for _ in range(n)]
    statuses = random.choices(["completed", "cancelled", "refunded"], weights=[0.7, 0.2, 0.1], k=n)

    df = pd.DataFrame({
        "ride_id": ride_ids,
        "customer_id": customer_ids,
        "amount": amounts,
        "timestamp": timestamps,
        "status": statuses
    })
    return df

df_parquet = generate_parquet_data(num_rows)
parquet_file_path = "data/raw_sales.parquet"
df_parquet.to_parquet(parquet_file_path, index=False)
print(f"[✓] Parquet file created at {parquet_file_path}")

# -------- Create Large SQLite Database --------
def generate_sql_data(n):
    data = []
    for i in range(n):
        order_id = 3000 + i
        customer_name = fake.first_name()
        total = random.randint(100, 3000)
        order_date = datetime(2024, 11, 1) + timedelta(minutes=random.randint(0, 100000))
        channel = random.choice(["app", "website", "store"])
        data.append((order_id, customer_name, total, order_date.strftime("%Y-%m-%d %H:%M:%S"), channel))
    return data

conn = sqlite3.connect("data/sales.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS online_sales (
        order_id INTEGER PRIMARY KEY,
        customer_name TEXT,
        total INTEGER,
        order_date TEXT,
        channel TEXT
    )
""")
sql_data = generate_sql_data(num_rows)
cursor.executemany("""
    INSERT INTO online_sales (order_id, customer_name, total, order_date, channel)
    VALUES (?, ?, ?, ?, ?)
""", sql_data)
conn.commit()
conn.close()

print("[✓] SQLite database created at data/sales.db with 5000 rows")
