import os
import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

DATA_DIR = "data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

def generate_new_transactions(n=100):
    records = []
    for _ in range(n):
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"CUST_{random.randint(1, 50)}",
            "product_id": f"P{random.randint(1, 10):03d}",
            "amount": round(random.uniform(10, 500), 2),
            "transaction_date": datetime.now().date(),
            "status": random.choice(["completed", "pending"]),
            "updated_at": datetime.now()
        })
    return pd.DataFrame(records)

def update_existing_transactions():
    all_files = sorted(os.listdir(DATA_DIR))
    if not all_files:
        return pd.DataFrame()

    latest_file = os.path.join(DATA_DIR, all_files[-1])
    df = pd.read_csv(latest_file)

    if df.empty:
        return pd.DataFrame()

    sample = df.sample(min(10, len(df))).copy()
    sample["amount"] = sample["amount"] + random.uniform(1, 50)
    sample["status"] = random.choice(["completed", "refunded"])
    sample["updated_at"] = datetime.now()

    return sample

def main():
    today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(DATA_DIR, f"daily_transactions_{today_str}.csv")

    new_data = generate_new_transactions()
    updated_data = update_existing_transactions()

    final_df = pd.concat([new_data, updated_data], ignore_index=True)

    final_df.to_csv(file_path, index=False)

    print(f"Generated file: {file_path}")
    print(f"New records: {len(new_data)}")
    print(f"Updated records: {len(updated_data)}")

if __name__ == "__main__":
    main()
