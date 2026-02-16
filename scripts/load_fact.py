import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from data_quality import run_data_quality_checks, fetch_valid_products

logging.basicConfig(level=logging.INFO)

DB_CONFIG = {
    "host": "warehouse-db",
    "database": "warehouse",
    "user": "warehouse_user",
    "password": "warehouse_pass",
    "port": 5432
}


def fetch_staging_data(conn):
    return pd.read_sql("SELECT * FROM staging.transactions;", conn)


def load_quarantine(conn, invalid_df):
    if invalid_df.empty:
        logging.info("No invalid records to quarantine.")
        return

    cursor = conn.cursor()

    records = list(invalid_df.itertuples(index=False, name=None))

    query = """
        INSERT INTO quarantine.transactions_errors
        (transaction_id, customer_id, product_id, amount,
         transaction_date, status, updated_at, error_message)
        VALUES %s
        ON CONFLICT (transaction_id, error_message)
        DO NOTHING;
    """

    execute_values(cursor, query, records)
    conn.commit()
    cursor.close()

    logging.info("Quarantine load completed.")


def load_fact_table(conn, valid_df):
    if valid_df.empty:
        logging.info("No valid records to load into fact table.")
        return

    cursor = conn.cursor()

    records = list(valid_df.itertuples(index=False, name=None))

    query = """
        INSERT INTO public.fact_transactions
        (transaction_id, customer_id, product_id, amount,
         transaction_date, status, updated_at)
        VALUES %s
        ON CONFLICT (transaction_id, transaction_date)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
        WHERE public.fact_transactions.updated_at < EXCLUDED.updated_at;
    """

    execute_values(cursor, query, records)
    conn.commit()
    cursor.close()

    logging.info("Fact table load completed.")


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        staging_df = fetch_staging_data(conn)

        if staging_df.empty:
            logging.info("No records in staging.")
            return

        valid_products = fetch_valid_products(conn)
        valid_df, invalid_df = run_data_quality_checks(staging_df, valid_products)

        logging.info(f"Valid records: {len(valid_df)}")
        logging.info(f"Invalid records: {len(invalid_df)}")

        load_quarantine(conn, invalid_df)
        load_fact_table(conn, valid_df)

        logging.info("Load process completed successfully.")

    except Exception:
        logging.exception("Load process failed")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
