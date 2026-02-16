import pandas as pd
from scripts.data_quality import run_data_quality_checks

def test_valid_data_passes():
    df = pd.DataFrame([
        {
            "transaction_id": "T1",
            "customer_id": "C1",
            "product_id": "P001",
            "amount": 100.0,
            "transaction_date": "2026-02-16",
            "status": "completed",
            "updated_at": "2026-02-16 10:00:00"
        }
    ])

    valid_products = ["P001"]

    valid_df, invalid_df = run_data_quality_checks(df, valid_products)

    assert len(valid_df) == 1
    assert len(invalid_df) == 0


def test_invalid_amount_fails():
    df = pd.DataFrame([
        {
            "transaction_id": "T2",
            "customer_id": "C1",
            "product_id": "P001",
            "amount": -10.0,
            "transaction_date": "2026-02-16",
            "status": "completed",
            "updated_at": "2026-02-16 10:00:00"
        }
    ])

    valid_products = ["P001"]

    valid_df, invalid_df = run_data_quality_checks(df, valid_products)

    assert len(valid_df) == 0
    assert len(invalid_df) == 1


def test_invalid_product_fails():
    df = pd.DataFrame([
        {
            "transaction_id": "T3",
            "customer_id": "C1",
            "product_id": "P999",
            "amount": 50.0,
            "transaction_date": "2026-02-16",
            "status": "completed",
            "updated_at": "2026-02-16 10:00:00"
        }
    ])

    valid_products = ["P001"]

    valid_df, invalid_df = run_data_quality_checks(df, valid_products)

    assert len(valid_df) == 0
    assert len(invalid_df) == 1
