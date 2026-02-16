import pandas as pd
from datetime import datetime

def test_incremental_logic():
    source_df = pd.DataFrame([
        {"transaction_id": "T1", "updated_at": datetime(2026, 2, 16, 10)},
        {"transaction_id": "T2", "updated_at": datetime(2026, 2, 16, 11)},
    ])

    existing_df = pd.DataFrame([
        {"transaction_id": "T1", "updated_at": datetime(2026, 2, 16, 9)},
    ])

    merged = source_df.merge(
        existing_df,
        on="transaction_id",
        how="left",
        suffixes=("", "_existing")
    )

    incremental_df = merged[
        merged["updated_at_existing"].isna() |
        (merged["updated_at"] > merged["updated_at_existing"])
    ]

    assert len(incremental_df) == 2
