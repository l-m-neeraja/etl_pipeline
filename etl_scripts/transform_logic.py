import pandas as pd

def calculate_total_order_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the total_order_value column by multiplying item_price and quantity.
    """
    if df.empty:
        # Avoid pandas warnings on empty df slice assignment
        df_copy = df.copy()
        df_copy['total_order_value'] = pd.Series(dtype='float64')
        return df_copy

    df_copy = df.copy()
    # Coerce to float/numeric to make sure multiplication works correctly
    price = pd.to_numeric(df_copy['item_price'], errors='coerce')
    qty = pd.to_numeric(df_copy['quantity'], errors='coerce')
    
    df_copy['total_order_value'] = price * qty
    return df_copy
