import pandas as pd
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)

DB_CONFIG = {
    "host": "warehouse-db",
    "database": "warehouse",
    "user": "warehouse_user",
    "password": "warehouse_pass",
    "port": 5432
}


EXPECTED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "amount",
    "transaction_date",
    "status",
    "updated_at"
]


def fetch_staging_data(conn):
    query = "SELECT * FROM staging.transactions;"
    return pd.read_sql(query, conn)


def fetch_valid_products(conn):
    query = "SELECT product_id FROM public.products;"
    return pd.read_sql(query, conn)["product_id"].tolist()


def run_data_quality_checks(df, valid_products):
    errors = []

    # 1️⃣ Schema check
    if set(EXPECTED_COLUMNS) != set(df.columns):
        raise ValueError("Schema mismatch in staging table")

    # 2️⃣ Uniqueness check
    duplicate_ids = df[df.duplicated("transaction_id", keep=False)]
    for _, row in duplicate_ids.iterrows():
        errors.append((row, "Duplicate transaction_id"))

    # 3️⃣ Completeness check
    null_check = df[
        df[["transaction_id", "customer_id", "product_id", "amount", "transaction_date"]]
        .isnull()
        .any(axis=1)
    ]
    for _, row in null_check.iterrows():
        errors.append((row, "Null value in required fields"))

    # 4️⃣ Validity check
    invalid_amount = df[df["amount"] <= 0]
    for _, row in invalid_amount.iterrows():
        errors.append((row, "Amount must be greater than 0"))

    # 5️⃣ Referential integrity
    invalid_product = df[~df["product_id"].isin(valid_products)]
    for _, row in invalid_product.iterrows():
        errors.append((row, "Invalid product_id"))

    # Build invalid_df
    invalid_rows = []
    for row, msg in errors:
        record = row.to_dict()
        record["error_message"] = msg
        invalid_rows.append(record)

    invalid_df = pd.DataFrame(invalid_rows)

    if not invalid_df.empty:
        valid_df = df[~df["transaction_id"].isin(invalid_df["transaction_id"])]
    else:
        valid_df = df.copy()

    return valid_df, invalid_df


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        df = fetch_staging_data(conn)

        if df.empty:
            logging.info("No records in staging.")
            return

        valid_products = fetch_valid_products(conn)

        valid_df, invalid_df = run_data_quality_checks(df, valid_products)

        logging.info(f"Valid records: {len(valid_df)}")
        logging.info(f"Invalid records: {len(invalid_df)}")

    except Exception as e:
        logging.exception("Data quality check failed")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
