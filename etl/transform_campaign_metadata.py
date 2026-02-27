import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_campaign_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Google Ads campaign metadata
    ---
    Principles:
        1. Validate input DataFrame
        2. Validate required identifier columns
        3. Enrich platform dimension
        4. Parse structured campaign_name
        5. Finalize normalized metadata schema
    ---
    Returns:
        1. DataFrame:
            Enforced campaign metadata records
    """

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Google Ads campaign metadata..."
    )

    if df.empty:
        print("⚠️ [TRANSFORM] Empty campaign metadata then transformation will be suspended.")
        return df

    required_cols = {
        "customer_id",
        "campaign_id",
        "campaign_name"
        }
    
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError (
            "❌ [TRANSFORM] Faile to transform Google Ads campaign metadata due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    df["platform"] = "Google"
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),
        track=df["campaign_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        group=df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content=df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),
    )
    
    print(
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Google Ads campaign metadata."
    )

    return df