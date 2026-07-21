import os
import sys
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Ensure the parent directory is in python path
sys.path.append("/opt/airflow")

from etl_scripts.transform_logic import calculate_total_order_value
from etl_scripts.data_validation import validate_records

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def ingest_raw_data(**kwargs):
    """
    Scans data/raw_orders/ for csv files, identifies unprocessed files,
    coerces columns to prevent DB insert errors, and inserts/upserts records 
    into raw_orders table.
    """
    logger = logging.getLogger("airflow.task")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_ecommerce_data')
    
    # 1. Fetch already ingested files
    query_processed = "SELECT file_name FROM processed_files"
    try:
        processed_files = {row[0] for row in mysql_hook.get_records(query_processed)}
    except Exception as e:
        logger.warning(f"Could not fetch processed files (it might be the first run): {e}")
        processed_files = set()
        
    # 2. Scan directory
    input_dir = "/opt/airflow/data/raw_orders"
    if not os.path.exists(input_dir):
        logger.info(f"Input directory {input_dir} does not exist. Creating it.")
        os.makedirs(input_dir, exist_ok=True)
        
    all_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    new_files = sorted([f for f in all_files if f not in processed_files])
    
    if not new_files:
        logger.info("No new files found to ingest.")
        kwargs['ti'].xcom_push(key='ingested_count', value=0)
        return
        
    logger.info(f"Found {len(new_files)} new files to ingest: {new_files}")
    
    total_ingested = 0
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    load_time = datetime.utcnow()
    load_time_str = load_time.strftime('%Y-%m-%d %H:%M:%S')
    
    insert_sql = """
    INSERT INTO raw_orders (order_id, customer_id, product_name, item_price, quantity, order_date, load_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        customer_id = VALUES(customer_id),
        product_name = VALUES(product_name),
        item_price = VALUES(item_price),
        quantity = VALUES(quantity),
        order_date = VALUES(order_date),
        load_timestamp = VALUES(load_timestamp)
    """
    
    track_file_sql = "INSERT INTO processed_files (file_name, ingested_at) VALUES (%s, %s)"
    
    for file_name in new_files:
        file_path = os.path.join(input_dir, file_name)
        logger.info(f"Ingesting file: {file_name}")
        
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Failed to read file {file_name}: {e}")
            continue
            
        records = []
        seen_ids = {} # For handling duplicates within the batch
        
        for idx, row in df.iterrows():
            orig_order_id = row.get('order_id')
            cust_id = row.get('customer_id')
            prod_name = row.get('product_name')
            price = row.get('item_price')
            qty = row.get('quantity')
            date = row.get('order_date')
            
            # Handle null order_id using a placeholder to bypass PK constraint in raw_orders
            if pd.isna(orig_order_id) or str(orig_order_id).strip() == '':
                order_id = f"MISSING_ID_{idx}_{int(datetime.utcnow().timestamp())}"
            else:
                order_id = str(orig_order_id).strip()
                # Handle duplicates within this CSV file using a placeholder suffix
                if order_id in seen_ids:
                    seen_ids[order_id] += 1
                    order_id = f"{order_id}_DUP_{seen_ids[order_id]}"
                else:
                    seen_ids[order_id] = 0
            
            # Coerce fields to safe types
            cust_id = None if pd.isna(cust_id) else str(cust_id)
            prod_name = None if pd.isna(prod_name) else str(prod_name)
            
            try:
                price = float(price) if not pd.isna(price) else None
            except Exception:
                price = None
                
            try:
                qty = int(float(qty)) if not pd.isna(qty) else None
            except Exception:
                qty = None
                
            # Coerce order_date to valid datetime string or None
            if pd.isna(date):
                date_str = None
            elif isinstance(date, (pd.Timestamp, datetime)):
                date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                try:
                    parsed_date = pd.to_datetime(date)
                    if pd.isna(parsed_date):
                        date_str = None
                    else:
                        date_str = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    date_str = None
                
            records.append((order_id, cust_id, prod_name, price, qty, date_str, load_time_str))
            
        if records:
            try:
                # Bulk insert/upsert the file data
                cursor.executemany(insert_sql, records)
                # Track file as processed
                cursor.execute(track_file_sql, (file_name, load_time_str))
                conn.commit()
                total_ingested += len(records)
                logger.info(f"Successfully ingested {len(records)} records from {file_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to commit database changes for {file_name}: {e}")
                
    cursor.close()
    conn.close()
    
    # Required logging: output the count of records ingested
    logger.info(f"Total Ingestion Summary: Ingested {total_ingested} records into raw_orders.")
    kwargs['ti'].xcom_push(key='ingested_count', value=total_ingested)


def transform_and_validate_data(**kwargs):
    """
    Reads un-transformed records from raw_orders, runs data quality checks,
    sends invalid records to error_records quarantine table,
    calculates total_order_value for valid records, and writes valid records
    to a temporary CSV.
    """
    logger = logging.getLogger("airflow.task")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_ecommerce_data')
    
    # Query for records that are not in fact_orders and (if previously quarantined)
    # have been re-ingested with a newer load_timestamp than detected_at
    query = """
    SELECT r.order_id, r.customer_id, r.product_name, r.item_price, r.quantity, r.order_date, r.load_timestamp
    FROM raw_orders r
    LEFT JOIN fact_orders f ON r.order_id = f.order_id
    LEFT JOIN (
        SELECT JSON_UNQUOTE(JSON_EXTRACT(source_data, '$.order_id')) as error_order_id, MAX(detected_at) as max_detected_at
        FROM error_records
        GROUP BY error_order_id
    ) e ON r.order_id = e.error_order_id
    WHERE f.order_id IS NULL
      AND (e.error_order_id IS NULL OR r.load_timestamp > e.max_detected_at)
    """
    
    df = mysql_hook.get_pandas_df(query)
    
    if df.empty:
        logger.info("No new or updated records to transform and validate.")
        kwargs['ti'].xcom_push(key='temp_file_path', value=None)
        logger.info("Transform Summary: 0 transformed, 0 quarantined.")
        return
        
    logger.info(f"Loaded {len(df)} unprocessed records from raw_orders.")
    
    # Run validation checks
    valid_df, invalid_df = validate_records(df)
    
    # 1. Process Invalid Records (Quarantine)
    quarantine_count = len(invalid_df)
    if quarantine_count > 0:
        logger.warning(f"Found {quarantine_count} invalid records. Quarantining...")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        insert_error_sql = "INSERT INTO error_records (source_data, error_message, detected_at) VALUES (%s, %s, %s)"
        error_records_to_insert = []
        detected_time_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        for _, row in invalid_df.iterrows():
            # Clean up the JSON representation to match the raw original inputs
            raw_row = row.to_dict()
            error_msg = raw_row.pop('error_message', '')
            # Strip off metadata fields not in raw files
            raw_row.pop('load_timestamp', None)
            
            # Clean up order_id placeholders
            order_id_val = str(raw_row.get('order_id'))
            if order_id_val.startswith('MISSING_ID_'):
                raw_row['order_id'] = None
            elif '_DUP_' in order_id_val:
                raw_row['order_id'] = order_id_val.split('_DUP_')[0]
                
            source_data_json = json.dumps(raw_row, default=str)
            error_records_to_insert.append((source_data_json, error_msg, detected_time_str))
            
        try:
            cursor.executemany(insert_error_sql, error_records_to_insert)
            conn.commit()
            logger.info(f"Successfully quarantined {quarantine_count} records in error_records.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert quarantined records: {e}")
        finally:
            cursor.close()
            conn.close()
            
    # 2. Process Valid Records (Transformation)
    transform_count = len(valid_df)
    temp_file_path = None
    
    if transform_count > 0:
        logger.info(f"Found {transform_count} valid records. Applying business logic...")
        transformed_df = calculate_total_order_value(valid_df)
        
        # Save to a temporary CSV on the shared volume
        run_id = kwargs['run_id']
        # Clean run_id of characters that might be unsafe for file names
        safe_run_id = "".join([c if c.isalnum() else "_" for c in run_id])
        temp_dir = "/opt/airflow/data/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, f"valid_orders_{safe_run_id}.csv")
        
        transformed_df.to_csv(temp_file_path, index=False)
        logger.info(f"Saved {transform_count} transformed records to temp file: {temp_file_path}")
        
    # Required logging: output count of records successfully transformed and sent to quarantine
    logger.info(f"Transform Summary: Successfully transformed {transform_count} records. Quarantined {quarantine_count} records.")
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
    kwargs['ti'].xcom_push(key='quarantined_count', value=quarantine_count)
    kwargs['ti'].xcom_push(key='transformed_count', value=transform_count)


def load_fact_data_incrementally(**kwargs):
    """
    Pulls temporary CSV file path from XCom, loads the valid transformed data,
    and inserts it into the fact_orders analytical table.
    """
    logger = logging.getLogger("airflow.task")
    ti = kwargs['ti']
    temp_file_path = ti.xcom_pull(task_ids='transform_and_validate_data', key='temp_file_path')
    
    if not temp_file_path or not os.path.exists(temp_file_path):
        logger.info("No transformed data file received. Skipping load.")
        return
        
    logger.info(f"Reading transformed data from: {temp_file_path}")
    try:
        df = pd.read_csv(temp_file_path)
    except Exception as e:
        logger.error(f"Failed to read temp file {temp_file_path}: {e}")
        return
        
    if df.empty:
        logger.info("Transformed DataFrame is empty. Skipping load.")
        # Cleanup
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return
        
    mysql_hook = MySqlHook(mysql_conn_id='mysql_ecommerce_data')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Use INSERT ... ON DUPLICATE KEY UPDATE to prevent duplicate order_ids (idempotency)
    insert_fact_sql = """
    INSERT INTO fact_orders (order_id, customer_id, product_name, total_order_value, order_date, processed_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        customer_id = VALUES(customer_id),
        product_name = VALUES(product_name),
        total_order_value = VALUES(total_order_value),
        order_date = VALUES(order_date),
        processed_timestamp = VALUES(processed_timestamp)
    """
    
    records = []
    for _, row in df.iterrows():
        # Ensure order_date is correctly formatted string
        date_val = row.get('order_date')
        if pd.isna(date_val):
            date_str = None
        else:
            try:
                date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                date_str = str(date_val)
                
        records.append((
            str(row.get('order_id')),
            str(row.get('customer_id')),
            str(row.get('product_name')),
            float(row.get('total_order_value')),
            date_str,
            processed_time
        ))
        
    loaded_count = 0
    try:
        cursor.executemany(insert_fact_sql, records)
        conn.commit()
        loaded_count = len(records)
        logger.info(f"Successfully loaded {loaded_count} records into fact_orders.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load records into fact_orders: {e}")
    finally:
        cursor.close()
        conn.close()
        
    # Cleanup temporary file
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
        logger.info(f"Removed temporary file: {temp_file_path}")
        
    logger.info(f"Load Summary: Loaded {loaded_count} records into the analytical data warehouse.")


# Instantiate DAG
with DAG(
    dag_id='ecommerce_etl',
    default_args=default_args,
    description='Idempotent daily e-commerce order processing ETL pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_raw_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_and_validate_data',
        python_callable=transform_and_validate_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_fact_data_incrementally',
        python_callable=load_fact_data_incrementally,
        provide_context=True,
    )

    ingest_task >> transform_task >> load_task
