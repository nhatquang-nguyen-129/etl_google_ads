import logging
from pathlib import Path
import sys
from typing import (
    List, 
    Dict
)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

def extract_campaign_metadata(
    *,
    google_ads_credentials: Dict,
    customer_id: str,
    campaign_id_list: List[str]
) -> List[dict]:
    """
    Extract campaign metadata from Google Ads
    ---------
    Workflow:
        1. Initialize Google Ads client using provided credentials
        2. Execute GAQL query filtered by campaign_id list
        3. Normalize API response into Python dictionaries
        4. Return campaign metadata records
    ---------
    Parameters:
        1. campaign.id
        2. campaign.name
        3. campaign.status
        4. campaign.advertising_channel_type
        5. campaign.advertising_channel_sub_type
        6. campaign.serving_status
        7. campaign.start_date
        8. campaign.end_date
    ---------
    Returns:
        1. List[dict]: Flattened campaign metadata records
    """

    if not campaign_id_list:
        return []

    _CAMPAIGN_METADATA_QUERY = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.advertising_channel_sub_type,
            campaign.serving_status,
            campaign.start_date,
            campaign.end_date
        FROM campaign
        WHERE campaign.id IN ({",".join(map(str, campaign_id_list))})
    """

    # 1. Initialize
    google_ads_client = GoogleAdsClient.load_from_dict({
        **google_ads_credentials,
        "use_proto_plus": True
    })

    # 2. Make API call
    google_ads_service = google_ads_client.get_service("GoogleAdsService")

    rows: List[dict] = []

    try:
        response = google_ads_service.search(
            customer_id=customer_id,
            query=_CAMPAIGN_METADATA_QUERY
        )

        for row in response:
            rows.append({
                "customer_id": customer_id,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "campaign_status": row.campaign.status.name,
                "channel_type": row.campaign.advertising_channel_type.name,
                "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                "serving_status": row.campaign.serving_status.name,
                "start_date": row.campaign.start_date,
                "end_date": row.campaign.end_date,
            })

    except GoogleAdsException as e:
        msg = (
            f"❌ [EXTRACT] Failed to extract Google Ads campaign metadata for "
            f"{len(campaign_id_list)} campaign_id(s) due to "
            f"{e}."
        )
        raise RuntimeError(msg)

    return rows
