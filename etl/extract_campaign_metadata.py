import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import logging
from typing import List
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

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
        msg = (
            "⚠️ [EXTRACT] No Google Ads campaign id found for customer_id "
            f"{customer_id} then extraction will be suspended."
        )
        print(msg)
        logging.info(msg)
        return pd.DataFrame()
    
    campaign_ids_str = ", ".join([f"'{cid}'" for cid in campaign_id_list])

    _QUERY_CAMPAIGN_METADATA = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.serving_status,           
            customer.id
        FROM campaign
        WHERE campaign.id IN ({campaign_ids_str})
    """

    msg = (
        "🔍 [EXTRACT] Extracting Google Ads campaign metadata for customer_id "
        f"{customer_id} with "
        f"{len(campaign_id_list)} campaign_id(s)..."
    )
    print(msg)
    logging.info(msg)

    google_ads_service = google_ads_client.get_service("GoogleAdsService")
    response = google_ads_service.search(
        customer_id=customer_id,
        query=_QUERY_CAMPAIGN_METADATA,
    )

    rows: List[dict] = []

    for row in response:
        campaign = row.campaign

        rows.append({
            "customer_id": str(row.customer.id),
            "campaign_id": str(campaign.id),
            "campaign_name": campaign.name,
            "campaign_status": campaign.status.name,
            "serving_status": campaign.serving_status.name,
        })

    df = pd.DataFrame(rows)

    return df