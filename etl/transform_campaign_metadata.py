import os
import sys
import logging
import pandas as pd
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../"
        )
    )
)

def transform_campaign_metadata(input_campaign_metadatas: pd.DataFrame) -> pd.DataFrame:
    """
    Transform campaign metadata from Google Ads
    ---------
    Workflow:
        1. Validate input_campaign_metadatas
        2. Parse campaign metadata
        3. Drop raw columns
        4. Return campaign metadata records
    ---------
    Parameters:
        1. campaign.name
        2. campaign.start_date
        3. campaign.end_date
    ---------
    Returns:
        1. pd.DataFrame: Flattened campaign metadata records
    """

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(input_campaign_metadatas)} row(s) of Google Ads campaign metadata..."
        )
    print(msg)
    logging.info(msg)
    
    # Empty input
    if input_campaign_metadatas.empty:
        msg = ("⚠️ [TRANSFORM] Empty input campaign metadata then transformation will be skipped.")
        print(msg)
        logging.warning(msg)        
        return input_campaign_metadatas

    # Missing required column
    if "campaign_name" not in input_campaign_metadatas.columns:
        msg = ("⚠️ [TRANSFORM] Column campaign_name not found then transformation will be skipped.")
        print(msg)
        logging.warning(msg)        
        return input_campaign_metadatas

    # Transformation
    output_campaign_metadatas = input_campaign_metadatas.copy()    
    output_campaign_metadatas["platform"] = "Google"
    output_campaign_metadatas = output_campaign_metadatas.assign(
        objective=lambda output_campaign_metadatas: output_campaign_metadatas["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
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
        "✅ [TRANSFORM] Successfully transform "
        f"{len(output_campaign_metadatas)} row(s) of Google Ads campaign metadata."
        )
    print(msg)
    logging.warning(msg)   
    return output_campaign_metadatas