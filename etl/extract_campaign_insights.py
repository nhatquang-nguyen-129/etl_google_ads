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

def extract_campaign_insights(
    *,
    google_ads_credentials: Dict,
    customer_id: str,
    start_date: str,
    end_date: str
) -> List[dict]:
    """
    Extract Campaign Insights from Google Ads
    ---------
    Workflow:
        1. Initialize Google Ads client using provided credentials
        2. Execute GAQL query for campaign insights
        3. Stream & normalize results
            - Uses search_stream API for large datasets
            - Flattens API response into Python dictionaries
            - Converts cost from micros to standard currency unit
        4. Return extracted data
    ---------
    Grain:
        - Date
        - Customer
        - Campaign
    ---------
    Returns:
        List[dict]
            Flattened campaign insight records suitable for analytics pipelines
    """

    _CAMPAIGN_INSIGHTS_QUERY = """
        SELECT
            segments.date,
            customer.id,
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN @start_date AND @end_date
    """

    # 1. Init Google Ads client
    client = GoogleAdsClient.load_from_dict({
        **google_ads_credentials,
        "use_proto_plus": True
    })

    # 2. Prepare request
    google_ads_service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsStreamRequest")

    request.customer_id = customer_id
    request.query = _CAMPAIGN_INSIGHTS_QUERY
    request.parameter_values["start_date"].string_value = start_date
    request.parameter_values["end_date"].string_value = end_date

    rows: List[dict] = []

    # 3. Execute GAQL
    try:
        stream = google_ads_service.search_stream(request=request)

        for batch in stream:
            for row in batch.results:
                rows.append({
                    "date": row.segments.date,
                    "customer_id": row.customer.id,
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "campaign_status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "conversions": row.metrics.conversions,
                    "conversion_value": row.metrics.conversions_value,
                })

    except GoogleAdsException as e:
        raise RuntimeError(
            f"❌ [EXTRACT] Google Ads campaign insights failed: {e}"
        )

    return rows