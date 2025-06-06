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

# -------- Create Large Parquet Dataset with Extended Fields --------
def generate_parquet_data(n):
    ride_ids = range(1000, 1000 + n)
    customer_ids = [f"C{random.randint(100, 999)}" for _ in range(n)]
    trip_distances = [round(random.uniform(1.0, 30.0), 2) for _ in range(n)]
    fare_amounts = [round(dist * random.uniform(20, 60), 2) for dist in trip_distances]

    # Generate timestamps: pickup and dropoff
    base_time = datetime(2024, 11, 1)
    pickup_times = [base_time + timedelta(minutes=random.randint(0, 100000)) for _ in range(n)]
    dropoff_times = [pickup + timedelta(minutes=random.randint(5, 90)) for pickup in pickup_times]

    statuses = random.choices(["completed", "cancelled", "refunded"], weights=[0.7, 0.2, 0.1], k=n)

    df = pd.DataFrame({
        "ride_id": ride_ids,
        "customer_id": customer_ids,
        "trip_distance": trip_distances,
        "fare_amount": fare_amounts,
        "tpep_pickup_datetime": pickup_times,
        "tpep_dropoff_datetime": dropoff_times,
        "status": statuses
    })
    return df

df_parquet = generate_parquet_data(num_rows)
parquet_file_path = "data/raw_sales.parquet"
df_parquet.to_parquet(parquet_file_path, index=False)
print(f"[✓] Parquet file created at {parquet_file_path} with extra columns")

# -------- Create Large SQLite Database (unchanged) --------
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
cursor.execute("DROP TABLE IF EXISTS online_sales")
cursor.execute("""
    CREATE TABLE online_sales (
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
