import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

from plugins.google_bigquery import GoogleBigqueryLoader

def load_campaign_insights(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load Google Ads campaign insights
    ----------------------
    Workflow:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Make internalGoogleBigQueryLoader API call
    ---------
    Returns:
        None
    """    

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Google Ads campaign insights Dataframe then loading will be suspended.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Google Ads campaign insights to Google BigQuery table "
        f"{direction}..."
        )
    
    loader = GoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["date"],
        partition={
            "field": "date"
        },
        cluster=[
            "campaign_id"
        ],
    )