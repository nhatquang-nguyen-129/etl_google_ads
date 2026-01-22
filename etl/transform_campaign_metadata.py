import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import logging
import pandas as pd

def transform_campaign_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform campaign insights of Google Ads
    ---------
    Workflow:
        1. Validate required columns
        2. Parse campaign naming convention
        3. Enrich platform & date dimensions
        4. Return transformed DataFrame
    ---------
    Returns:
        1. DataFrame:
            Campaign metadata ready for ingestion
    """

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Google Ads campaign metadata..."
    )
    print(msg)
    logging.info(msg)

    if df.empty:
        msg = "⚠️ [TRANSFORM] Empty campaign metadata then transformation will be skipped."
        print(msg)
        logging.warning(msg)
        return df

    required_cols = {"campaign_name", "start_date"}
    missing = required_cols - set(df.columns)
    if missing:
        msg = (
            "⚠️ [TRANSFORM] Missing columns "
            f"{missing} of Google Ads campaign metadata then transformation will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return df

    df = df.copy()
    df["platform"] = "Google"
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),

        track_group=df["campaign_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar_group=df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content_group=df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),

        date=pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.floor("D"),
        year=pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.year,
        month=pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    )
    df = df.drop(
        columns=[
            "start_date", 
            "end_date"
        ], errors="ignore"
    )

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Google Ads campaign metadata."
    )
    print(msg)
    logging.info(msg)

    return df