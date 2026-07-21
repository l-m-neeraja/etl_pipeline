import pandas as pd
from datetime import datetime
from typing import Tuple

def validate_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates a DataFrame of orders against data quality rules:
    - Rule 1: order_id must not be null and must be unique within the batch.
    - Rule 2: item_price must be a positive numeric value (> 0).
    - Rule 3: quantity must be a positive numeric integer (> 0).
    - Rule 4: order_date must be a parseable and valid datetime format.
    
    Returns:
        (valid_df, invalid_df)
        invalid_df includes an 'error_message' column.
    """
    if df.empty:
        # Create empty dfs with appropriate columns
        empty_invalid = df.copy()
        empty_invalid['error_message'] = pd.Series(dtype='object')
        return df.copy(), empty_invalid

    valid_rows = []
    invalid_rows = []
    
    # Identify duplicate order_ids in the current batch (excluding nulls/empty strings)
    # We do a case-insensitive check and clean up whitespaces
    ids = df['order_id'].fillna('').astype(str).str.strip()
    non_empty_ids = ids[ids != '']
    duplicate_ids = set(non_empty_ids[non_empty_ids.duplicated()])
    
    for _, row in df.iterrows():
        errors = []
        
        # Rule 1: order_id must not be null and unique within the batch
        order_id = row.get('order_id')
        order_id_str = str(order_id).strip() if not pd.isna(order_id) else ''
        
        if order_id_str == '' or order_id_str.startswith('MISSING_ID_'):
            errors.append("order_id must not be null")
        elif '_DUP_' in order_id_str:
            orig_id = order_id_str.split('_DUP_')[0]
            errors.append(f"order_id '{orig_id}' is duplicate within the batch")
        elif order_id_str in duplicate_ids:
            errors.append(f"order_id '{order_id_str}' is duplicate within the batch")
                
        # Rule 2: item_price must be a positive numeric value (> 0)
        item_price = row.get('item_price')
        if pd.isna(item_price):
            errors.append("item_price must not be null")
        else:
            try:
                price_val = float(item_price)
                if price_val <= 0:
                    errors.append(f"item_price must be > 0 (got {price_val})")
            except (ValueError, TypeError):
                errors.append(f"item_price must be a positive numeric value (got '{item_price}')")
                
        # Rule 3: quantity must be a positive numeric integer (> 0)
        quantity = row.get('quantity')
        if pd.isna(quantity):
            errors.append("quantity must not be null")
        else:
            try:
                qty_val = float(quantity)
                # Check if it is an integer and > 0
                if qty_val <= 0:
                    errors.append(f"quantity must be > 0 (got {qty_val})")
                elif not qty_val.is_integer():
                    errors.append(f"quantity must be an integer (got {qty_val})")
            except (ValueError, TypeError):
                errors.append(f"quantity must be a positive numeric integer (got '{quantity}')")
                
        # Rule 4: order_date must be a parseable and valid datetime format
        order_date = row.get('order_date')
        if pd.isna(order_date):
            errors.append("order_date must not be null")
        else:
            if not isinstance(order_date, (pd.Timestamp, datetime)):
                try:
                    pd.to_datetime(order_date, errors='raise')
                except Exception:
                    errors.append(f"order_date must be a parseable and valid datetime format (got '{order_date}')")
                    
        if errors:
            err_row = row.to_dict()
            err_row['error_message'] = "; ".join(errors)
            invalid_rows.append(err_row)
        else:
            valid_rows.append(row.to_dict())
            
    # Reconstruct DataFrames
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)
        # Ensure order_date is datetime type
        valid_df['order_date'] = pd.to_datetime(valid_df['order_date'])
    else:
        valid_df = pd.DataFrame(columns=df.columns)
        
    if invalid_rows:
        invalid_df = pd.DataFrame(invalid_rows)
    else:
        invalid_df = pd.DataFrame(columns=list(df.columns) + ['error_message'])
        
    return valid_df, invalid_df
