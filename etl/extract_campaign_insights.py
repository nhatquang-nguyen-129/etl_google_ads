import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from typing import Dict, Any


def extract_campaign_insights(
    customer_id: str,
    oauth_config: Dict[str, Any],
    date_start: str,
    date_end: str,
    api_version: str = "v22",
) -> pd.DataFrame:
    """
    Extract Google Ads campaign-level insights.

    Parameters
    ----------
    customer_id : str
        Google Ads customer ID (without dashes)
    oauth_config : dict
        OAuth2 config loaded from secret / env
    date_start : str
        YYYY-MM-DD
    date_end : str
        YYYY-MM-DD
    api_version : str
        Google Ads API version

    Returns
    -------
    pd.DataFrame
        Campaign insights dataframe
    """

    client = GoogleAdsClient.load_from_dict(
        oauth_config,
        version=api_version
    )

    service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            campaign.id,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{date_start}' AND '{date_end}'
    """

    rows = []

    response = service.search(
        customer_id=customer_id,
        query=query
    )

    for row in response:
        rows.append({
            "platform": "Google",
            "customer_id": customer_id,
            "campaign_id": row.campaign.id,
            "stat_date": row.segments.date,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "conversions": row.metrics.conversions,
            "cost_micros": row.metrics.cost_micros,
        })

    return pd.DataFrame(rows)