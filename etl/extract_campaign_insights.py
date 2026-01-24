import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import logging
from typing import List
import pandas as pd
from google.ads.googleads.errors import GoogleAdsException

def extract_campaign_insights(
    *,
    google_ads_client,
    customer_id: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Extract Google Ads campaign insights
    ---------
    Workflow:
        1. Initialize Google Ads client
        2. Execute GAQL query for campaign insights
        3. Stream using search_stream API
        4. Return extracted data
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign insight records suitable for analytics pipelines
    """

    _QUERY_CAMPAIGN_INSIGHTS = f"""
        SELECT
            segments.date,
            customer.id,
            campaign.id,
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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
    request.query = _QUERY_CAMPAIGN_INSIGHTS

    rows: List[dict] = []
    batch_count = 0

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

        df = pd.DataFrame(rows)

        msg = (
            "✅ [EXTRACT] Successfully extracted "
            f"{len(df)} row(s) with {batch_count} batch(es)."
        )
        print(msg)
        logging.info(msg)

        return df

    except GoogleAdsException as e:
        for error in e.failure.errors:
            error_code = error.error_code
            if (
                error_code.internal_error
                or error_code.resource_exhausted
                or error_code.server_error
                or error_code.timeout_error
            ):
                
                retryable = True
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id " 
                    f"{customer_id} from "
                    f"{start_date} to "
                    f"{end_date} due to API error."
                ) from e

        retryable = False
        raise ValueError(
            "❌ [EXTRACT] Failed to extract Google Ads campaign insights for customer_id" 
            f"{customer_id} from "
            f"{start_date} to "  
            f"{end_date} due to "
            f"{e}."
        ) from e