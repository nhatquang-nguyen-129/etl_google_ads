import logging
import pandas as pd
from plugins.google_bigquery import GoogleBigqueryLoader

def load_campaign_metadata(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Google Ads campaign metadata loader
    ----------------------
    Workflow:
        - UPSERT by (date, campaign_id)
        - Re-runnable safely
        - Append after deleting conflicts
            Table grain:
        date + campaign_id
    """    

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Google Ads campaign metadata Dataframe then loading will be skipped.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Google Ads campaign metadata to Google BigQuery table "
        f"{direction}..."
        )
    GoogleBigqueryLoader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["date", "campaign_id"],
        partition={
            "field": "date"
        },
        cluster=[
            "campaign_id"
        ],
    )