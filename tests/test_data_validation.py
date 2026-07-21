import pandas as pd
import pytest
from datetime import datetime
from etl_scripts.data_validation import validate_records

def test_validation_all_valid():
    # Arrange
    data = {
        'order_id': ['ORD-1', 'ORD-2'],
        'customer_id': ['CUST-A', 'CUST-B'],
        'product_name': ['Widget A', 'Widget B'],
        'item_price': [10.50, 99.99],
        'quantity': [2, 1],
        'order_date': ['2023-10-01 12:00:00', datetime(2023, 10, 2, 14, 0)]
    }
    df = pd.DataFrame(data)
    
    # Act
    valid_df, invalid_df = validate_records(df)
    
    # Assert
    assert len(valid_df) == 2
    assert len(invalid_df) == 0
    assert 'error_message' not in valid_df.columns
    assert 'error_message' in invalid_df.columns

def test_validation_rule_1_order_id():
    # Arrange: null order_id, duplicate order_id, placeholder MISSING_ID, placeholder _DUP_
    data = {
        'order_id': [None, 'ORD-DUP', 'ORD-DUP', 'MISSING_ID_123', 'ORD-DUP2_DUP_1'],
        'customer_id': ['CUST-1', 'CUST-2', 'CUST-3', 'CUST-4', 'CUST-5'],
        'product_name': ['A', 'B', 'C', 'D', 'E'],
        'item_price': [10.00, 10.00, 10.00, 10.00, 10.00],
        'quantity': [1, 1, 1, 1, 1],
        'order_date': ['2023-10-01 12:00:00'] * 5
    }
    df = pd.DataFrame(data)
    
    # Act
    valid_df, invalid_df = validate_records(df)
    
    # Assert
    assert len(valid_df) == 0
    assert len(invalid_df) == 5
    
    # Check error messages
    err_msgs = invalid_df['error_message'].tolist()
    assert any("order_id must not be null" in msg for msg in err_msgs) # For None and MISSING_ID_123
    assert any("is duplicate within the batch" in msg for msg in err_msgs) # For ORD-DUP and ORD-DUP2_DUP_1

def test_validation_rule_2_item_price():
    # Arrange: null price, negative price, zero price, string price
    data = {
        'order_id': ['ORD-1', 'ORD-2', 'ORD-3', 'ORD-4'],
        'customer_id': ['CUST-1'] * 4,
        'product_name': ['A'] * 4,
        'item_price': [None, -5.00, 0.00, 'abc'],
        'quantity': [1] * 4,
        'order_date': ['2023-10-01 12:00:00'] * 4
    }
    df = pd.DataFrame(data)
    
    # Act
    valid_df, invalid_df = validate_records(df)
    
    # Assert
    assert len(valid_df) == 0
    assert len(invalid_df) == 4
    
    err_msgs = invalid_df['error_message'].tolist()
    assert "item_price must not be null" in err_msgs[0]
    assert "item_price must be > 0" in err_msgs[1]
    assert "item_price must be > 0" in err_msgs[2]
    assert "item_price must be a positive numeric value" in err_msgs[3]

def test_validation_rule_3_quantity():
    # Arrange: null quantity, negative quantity, float quantity, zero quantity, string quantity
    data = {
        'order_id': ['ORD-1', 'ORD-2', 'ORD-3', 'ORD-4', 'ORD-5'],
        'customer_id': ['CUST-1'] * 5,
        'product_name': ['A'] * 5,
        'item_price': [10.00] * 5,
        'quantity': [None, -1, 1.5, 0, 'abc'],
        'order_date': ['2023-10-01 12:00:00'] * 5
    }
    df = pd.DataFrame(data)
    
    # Act
    valid_df, invalid_df = validate_records(df)
    
    # Assert
    assert len(valid_df) == 0
    assert len(invalid_df) == 5
    
    err_msgs = invalid_df['error_message'].tolist()
    assert "quantity must not be null" in err_msgs[0]
    assert "quantity must be > 0" in err_msgs[1]
    assert "quantity must be an integer" in err_msgs[2]
    assert "quantity must be > 0" in err_msgs[3]
    assert "quantity must be a positive numeric integer" in err_msgs[4]

def test_validation_rule_4_order_date():
    # Arrange: null date, invalid date string
    data = {
        'order_id': ['ORD-1', 'ORD-2'],
        'customer_id': ['CUST-1', 'CUST-2'],
        'product_name': ['A', 'B'],
        'item_price': [10.00, 10.00],
        'quantity': [1, 1],
        'order_date': [None, 'not-a-date']
    }
    df = pd.DataFrame(data)
    
    # Act
    valid_df, invalid_df = validate_records(df)
    
    # Assert
    assert len(valid_df) == 0
    assert len(invalid_df) == 2
    
    err_msgs = invalid_df['error_message'].tolist()
    assert "order_date must not be null" in err_msgs[0]
    assert "order_date must be a parseable and valid datetime format" in err_msgs[1]
