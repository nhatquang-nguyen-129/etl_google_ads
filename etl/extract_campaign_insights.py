import logging
from pathlib import Path
import sys
from typing import List

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

def extract_campaign_insights(
    *,
    google_ads_client,
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

    msg = (
        "🔍 [EXTRACT] Extracting Google Ads campaign insights for customer_id "
        f"{customer_id} from start_date "
        f"{start_date} to end_date "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

    google_ads_service = google_ads_client.get_service("GoogleAdsService")
    request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")

    request.customer_id = customer_id
    request.query = _CAMPAIGN_INSIGHTS_QUERY
    request.parameter_values["start_date"].string_value = start_date
    request.parameter_values["end_date"].string_value = end_date

    rows: List[dict] = []

    try:
        stream = google_ads_service.search_stream(request=request)

        for batch in stream:
            batch_count += 1
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
        
        msg = (
            "✅ [EXTRACT] Successfully extracted "
            f"{len(rows)} row(s) of Google Ads campaign insights with "
            f"{batch_count} batch(es)."
        )
        print(msg)                
        logging.info(msg)

    except GoogleAdsException as e:
        for error in e.failure.errors:
            error_code = error.error_code
            if (
                error_code.internal_error
                or error_code.resource_exhausted
                or error_code.server_error
                or error_code.timeout_error
            ):
                
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id " 
                    f"{customer_id} from "
                    f"{start_date} to "
                    f"{end_date} due to API error."
                ) from e

        raise ValueError(
            "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id" 
            f"{customer_id} from "
            f"{start_date} to "  
            f"{end_date} due to "
            f"{e}."
        ) from e