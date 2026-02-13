import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

def transform_campaign_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Google Ads campaign insights
    ---------
    Workflow:
        1. Validate required columns
        2. Normalize date dimensions
        3. Enforce metrics' schema
    ---------
    Returns:
        1. DataFrame:
            Enforced campaign insights    
    """

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Google Ads campaign insights..."
    )
    print(msg)
    logging.info(msg)

    if df.empty:
        msg = "⚠️ [TRANSFORM] Empty campaign insights then transformation will be suspended."
        print(msg)
        logging.warning(msg)
        return df

    required_cols = {"date"}

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            "❌ [TRANSFORM] Failed to transform Google Ads campaign insights due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    df["customer_id"] = df["customer_id"].astype(str)
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["impressions"] = df["impressions"].astype("int64")
    df["clicks"] = df["clicks"].astype("int64")
    df["spend"] = (df["cost"] * 100).round().astype("int64")
    df["conversions"] = df["conversions"].astype("float64")
    df["conversion_value"] = df["conversion_value"].astype("float64")    
    df = df.assign(
        date=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.floor("D"),
        year=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.year,
        month=pd.to_datetime(df["date"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    )

    df = df.drop(columns=["cost"])

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Google Ads campaign insights."
    )
    print(msg)
    logging.info(msg)

    return df