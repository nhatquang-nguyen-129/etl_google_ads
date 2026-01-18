import os
import sys
import logging
from typing import (
    List, 
    Dict
)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../"
        )
    )
)

def extract_campaign_metadata(
    *,
    google_ads_credentials: Dict,
    customer_id: str,
    campaign_id_list: List[str]
) -> List[dict]:
    """
    Extract Campaign Metadata from Google Ads
    ----------------------------------------
    Purpose:
        Retrieve campaign-level metadata (non-metric attributes)
        for enrichment and dimensional modeling.

    Workflow:
        1. Initialize Google Ads client using provided credentials
        2. Execute GAQL query filtered by campaign_id list
        3. Normalize API response into Python dictionaries
        4. Return campaign metadata records

    ----------------------------------------
    Grain:
        - Campaign
        - Customer (Google Ads account)

    ----------------------------------------
    Key Fields:
        - campaign.id
        - campaign.name
        - campaign.status
        - campaign.advertising_channel_type
        - campaign.advertising_channel_sub_type
        - campaign.serving_status
        - campaign.start_date
        - campaign.end_date

    ----------------------------------------
    Returns:
        List[dict]
            Flattened campaign metadata records
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

    # 1. Init Google Ads client
    client = GoogleAdsClient.load_from_dict({
        **google_ads_credentials,
        "use_proto_plus": True
    })

    # 2. Execute query
    google_ads_service = client.get_service("GoogleAdsService")

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
        raise RuntimeError(
            f"❌ [EXTRACT] Google Ads campaign metadata failed: {e}"
        )

    return rows
