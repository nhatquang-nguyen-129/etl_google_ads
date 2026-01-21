import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd
from pathlib import Path
from typing import (
    Union, 
    List, 
    Dict
)

def transform_campaign_metadata(
    input_campaign_metadatas: Union[pd.DataFrame, List[Dict]]
) -> pd.DataFrame:


    if isinstance(input_campaign_metadatas, list):
        input_campaign_metadatas = pd.DataFrame(input_campaign_metadatas)

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(input_campaign_metadatas)} row(s) of Google Ads campaign metadata..."
    )
    print(msg)
    logging.info(msg)

    if input_campaign_metadatas.empty:
        msg = "⚠️ [TRANSFORM] Empty input campaign metadata then transformation will be skipped."
        print(msg)
        logging.warning(msg)
        return input_campaign_metadatas

    required_cols = {"campaign_name", "start_date"}
    missing = required_cols - set(input_campaign_metadatas.columns)
    if missing:
        msg = f"⚠️ [TRANSFORM] Missing columns {missing} then transformation will be skipped."
        print(msg)
        logging.warning(msg)
        return input_campaign_metadatas

    df = input_campaign_metadatas.copy()
    df["platform"] = "Google"

    df = df.assign(
        objective=lambda df: df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=lambda df: df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=lambda df: df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=lambda df: df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),

        track_group=lambda df: df["campaign_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar_group=lambda df: df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content_group=lambda df: df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),

        date=lambda df: pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.floor("D"),
        year=lambda df: pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.strftime("%Y"),
        month=lambda df: pd.to_datetime(df["start_date"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    ).drop(columns=["start_date", "end_date"], errors="ignore")

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Google Ads campaign metadata."
    )
    print(msg)
    logging.info(msg)

    return df