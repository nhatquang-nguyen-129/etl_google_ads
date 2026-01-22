import logging
import pandas as pd
from plugins.google_bigquery import GoogleBigqueryLoader

def load_campaign_insights(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Google Ads campaign insights loader
    ----------------------
    Workflow:
        - UPSERT by (date, campaign_id)
        - Re-runnable safely
        - Append after deleting conflicts
            Table grain:
        date + campaign_id
    """    

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Google Ads campaign insights Dataframe then loading will be skipped.")
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
