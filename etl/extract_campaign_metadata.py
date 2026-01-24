import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import logging
from typing import List
import pandas as pd
from google.ads.googleads.errors import GoogleAdsException

def extract_campaign_metadata(
    google_ads_client,
    customer_id: str,
    campaign_id_list: List[str],
) -> pd.DataFrame:
    """
    Extract Google Ads campaign metadata
    ---------
    Workflow:
        1. Initialize Google Ads client
        2. Execute GAQL query for campaign metadata
        3. Stream using search_stream API
        4. Return extracted data
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign metadata records
    """

    if not campaign_id_list:
        msg = (
            "⚠️ [EXTRACT] No Google Ads campaign id found for customer_id "
            f"{customer_id} then extraction will be suspended."
        )
        print(msg)
        logging.info(msg)
        return pd.DataFrame()
    
    campaign_ids_str = ", ".join(str(cid) for cid in campaign_id_list)

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

    rows: List[dict] = []

    try:        
        response = google_ads_service.search(
            customer_id=customer_id,
            query=_QUERY_CAMPAIGN_METADATA,
        )       

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
                    "❌ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
                    f"{customer_id} due to retryable API error."
                ) from e

        retryable = False
        raise ValueError(
            "❌ [EXTRACT] Failed to extract Google Ads campaign metadata for customer_id "
            f"{customer_id} due to non-retryable error: "
            f"{e}."
        ) from e