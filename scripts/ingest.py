import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

DATA_DIR = "data/raw"

DB_CONFIG = {
    "host": "warehouse-db",
    "database": "warehouse",
    "user": "warehouse_user",
    "password": "warehouse_pass",
    "port": 5432
}


def get_latest_file():
    files = sorted(os.listdir(DATA_DIR))
    if not files:
        return None
    return os.path.join(DATA_DIR, files[-1])


def fetch_existing_records(conn):
    query = "SELECT transaction_id, updated_at FROM staging.transactions;"
    return pd.read_sql(query, conn)


def main():
    file_path = get_latest_file()
    if not file_path:
        logging.info("No source files found.")
        return

    logging.info(f"Processing file: {file_path}")
    df = pd.read_csv(file_path)

    if df.empty:
        logging.info("Source file is empty.")
        return

    df["updated_at"] = pd.to_datetime(df["updated_at"])

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        try:
            existing_df = fetch_existing_records(conn)
        except:
            existing_df = pd.DataFrame(columns=["transaction_id", "updated_at"])

        if not existing_df.empty:
            existing_df["updated_at"] = pd.to_datetime(existing_df["updated_at"])
            merged = df.merge(existing_df, on="transaction_id", how="left", suffixes=("", "_existing"))

            incremental_df = merged[
                merged["updated_at_existing"].isna() |
                (merged["updated_at"] > merged["updated_at_existing"])
            ].drop(columns=["updated_at_existing"])
        else:
            incremental_df = df

        if incremental_df.empty:
            logging.info("No new or updated records to process.")
            return

        records = list(incremental_df.itertuples(index=False, name=None))

        insert_query = """
            INSERT INTO staging.transactions
            (transaction_id, customer_id, product_id, amount,
             transaction_date, status, updated_at)
            VALUES %s
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                amount = EXCLUDED.amount,
                transaction_date = EXCLUDED.transaction_date,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
            WHERE staging.transactions.updated_at < EXCLUDED.updated_at;
        """

        execute_values(cursor, insert_query, records)
        conn.commit()

        logging.info(f"Processed records: {len(incremental_df)}")

    except Exception as e:
        logging.exception("Ingestion failed")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
