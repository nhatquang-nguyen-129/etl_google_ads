import logging
from pathlib import Path
import sys
from typing import List

import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

def extract_campaign_metadata(
    google_ads_client,
    customer_id: str,
    campaign_id_list: List[str],
) -> pd.DataFrame:
    """
    Extract Campaign Metadata from Google Ads
    ---------
    Workflow:
        1. Initialize Google Ads client using provided credentials
        2. Execute GAQL query to fetch campaign-level attributes
        3. Normalize API response into tabular structure
        4. Return extracted metadata
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign metadata records suitable for
            dimension tables and downstream joins
    """

    if not campaign_id_list:
        return pd.DataFrame()

    google_ads_service = google_ads_client.get_service("GoogleAdsService")

    campaign_ids_str = ", ".join([f"'{cid}'" for cid in campaign_id_list])

    _QUERY_CAMPAIGN_METADATA = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.advertising_channel_sub_type,

            campaign.start_date,
            campaign.end_date,

            campaign.bidding_strategy_type,
            campaign.manual_cpc.enhanced_cpc_enabled,

            campaign.target_cpa.target_cpa_micros,
            campaign.target_roas.target_roas,

            campaign.serving_status,
            campaign.experiment_type,

            campaign.tracking_url_template,
            campaign.final_url_suffix,

            customer.id
        FROM campaign
        WHERE campaign.id IN ({campaign_ids_str})
    """

    response = ga_service.search(
        customer_id=customer_id,
        query=query,
    )

    rows: List[dict] = []

    for row in response:
        campaign = row.campaign

        rows.append({
            # Identifiers
            "customer_id": str(row.customer.id),
            "campaign_id": str(campaign.id),
            "campaign_name": campaign.name,

            # Status
            "campaign_status": campaign.status.name,
            "serving_status": campaign.serving_status.name,
            "experiment_type": campaign.experiment_type.name,

            # Channel
            "channel_type": campaign.advertising_channel_type.name,
            "channel_sub_type": campaign.advertising_channel_sub_type.name,

            # Dates
            "start_date": campaign.start_date,
            "end_date": campaign.end_date,

            # Bidding
            "bidding_strategy_type": campaign.bidding_strategy_type.name,
            "enhanced_cpc_enabled": (
                campaign.manual_cpc.enhanced_cpc_enabled
                if campaign.bidding_strategy_type.name == "MANUAL_CPC"
                else None
            ),
            "target_cpa": (
                campaign.target_cpa.target_cpa_micros / 1_000_000
                if campaign.target_cpa.target_cpa_micros
                else None
            ),
            "target_roas": (
                campaign.target_roas.target_roas
                if campaign.target_roas.target_roas
                else None
            ),

            # Tracking
            "tracking_url_template": campaign.tracking_url_template,
            "final_url_suffix": campaign.final_url_suffix,
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["target_cpa"] = df["target_cpa"].astype("float64")
        df["target_roas"] = df["target_roas"].astype("float64")

    return df