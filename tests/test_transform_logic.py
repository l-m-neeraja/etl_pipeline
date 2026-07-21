import pandas as pd
import pytest
from etl_scripts.transform_logic import calculate_total_order_value

def test_calculate_total_order_value_success():
    # Arrange
    data = {
        'order_id': ['ORD-1', 'ORD-2'],
        'item_price': [10.50, 20.00],
        'quantity': [2, 3]
    }
    df = pd.DataFrame(data)
    
    # Act
    result_df = calculate_total_order_value(df)
    
    # Assert
    assert 'total_order_value' in result_df.columns
    assert result_df.loc[0, 'total_order_value'] == 21.00
    assert result_df.loc[1, 'total_order_value'] == 60.00

def test_calculate_total_order_value_with_coercion():
    # Arrange
    # Pass strings that represent numbers to verify coercion works
    data = {
        'order_id': ['ORD-1'],
        'item_price': ['10.50'],
        'quantity': ['5']
    }
    df = pd.DataFrame(data)
    
    # Act
    result_df = calculate_total_order_value(df)
    
    # Assert
    assert result_df.loc[0, 'total_order_value'] == 52.50

def test_calculate_total_order_value_empty():
    # Arrange
    df = pd.DataFrame(columns=['order_id', 'item_price', 'quantity'])
    
    # Act
    result_df = calculate_total_order_value(df)
    
    # Assert
    assert 'total_order_value' in result_df.columns
    assert result_df.empty
