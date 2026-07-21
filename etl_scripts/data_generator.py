import os
import csv
import random
from datetime import datetime, timedelta

def generate_orders_data(output_dir="data/raw_orders"):
    """
    Generates 5 days of synthetic order files (CSV) and saves them in output_dir.
    Purposely injects bad rows (null/duplicate order_ids, negative prices/quantities, invalid dates).
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 5 target dates: from 4 days ago to today
    base_date = datetime.utcnow()
    dates = [base_date - timedelta(days=i) for i in range(4, -1, -1)]
    
    products = [
        ("Wireless Mouse", 25.50),
        ("Mechanical Keyboard", 89.99),
        ("USB-C Hub", 35.00),
        ("Noise Cancelling Headphones", 199.99),
        ("Ergonomic Office Chair", 249.50),
        ("Dual Monitor Stand", 59.99),
    ]
    
    filenames = []
    
    for day in dates:
        date_str = day.strftime("%Y%m%d")
        file_path = os.path.join(output_dir, f"orders_{date_str}.csv")
        filenames.append(file_path)
        
        # Determine day's date prefix
        day_date_only = day.strftime("%Y-%m-%d")
        
        rows = []
        
        # 1. Generate 10 good rows
        for i in range(10):
            order_id = f"ORD-{date_str}-{1000 + i}"
            customer_id = f"CUST-{random.randint(10000, 99999)}"
            prod_name, prod_price = random.choice(products)
            # Add small price fluctuation
            price = round(prod_price * random.uniform(0.9, 1.1), 2)
            quantity = random.randint(1, 5)
            # Random hour/minute/second on that day
            order_time = f"{day_date_only} {random.randint(0,23):02}:{random.randint(0,59):02}:{random.randint(0,59):02}"
            
            rows.append({
                "order_id": order_id,
                "customer_id": customer_id,
                "product_name": prod_name,
                "item_price": price,
                "quantity": quantity,
                "order_date": order_time
            })
            
        # 2. Inject bad data rows (different types of bad data)
        # Type A: Null/empty order_id
        rows.append({
            "order_id": "",
            "customer_id": "CUST-99999",
            "product_name": "USB-C Hub",
            "item_price": 35.00,
            "quantity": 1,
            "order_date": f"{day_date_only} 12:00:00"
        })
        
        # Type B: Duplicate order_id within the same batch
        # We will duplicate the first order_id
        if rows:
            dup_id = rows[0]["order_id"]
            rows.append({
                "order_id": dup_id,
                "customer_id": "CUST-88888",
                "product_name": "Wireless Mouse",
                "item_price": 25.50,
                "quantity": 2,
                "order_date": f"{day_date_only} 12:05:00"
            })
            
        # Type C: Negative/zero item_price
        rows.append({
            "order_id": f"ORD-{date_str}-BADPRICE",
            "customer_id": "CUST-77777",
            "product_name": "Mechanical Keyboard",
            "item_price": -10.00,
            "quantity": 1,
            "order_date": f"{day_date_only} 12:10:00"
        })
        
        # Type D: Zero/negative quantity
        rows.append({
            "order_id": f"ORD-{date_str}-BADQTY",
            "customer_id": "CUST-66666",
            "product_name": "Dual Monitor Stand",
            "item_price": 59.99,
            "quantity": 0,
            "order_date": f"{day_date_only} 12:15:00"
        })
        
        # Type E: Unparseable/invalid date format
        rows.append({
            "order_id": f"ORD-{date_str}-BADVALDATE",
            "customer_id": "CUST-55555",
            "product_name": "Ergonomic Office Chair",
            "item_price": 249.50,
            "quantity": 1,
            "order_date": "INVALID_DATE_STRING"
        })
        
        # Shuffle to mix up bad and good rows
        random.shuffle(rows)
        
        # Write to CSV
        with open(file_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["order_id", "customer_id", "product_name", "item_price", "quantity", "order_date"])
            writer.writeheader()
            writer.writerows(rows)
            
    print(f"Generated {len(dates)} files in '{output_dir}'.")
    return filenames

if __name__ == "__main__":
    import sys
    # If output directory is passed as an argument
    out = sys.argv[1] if len(sys.argv) > 1 else "data/raw_orders"
    generate_orders_data(out)
