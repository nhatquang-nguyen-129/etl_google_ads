import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
from datetime import datetime, timedelta
import logging
import time

from etl.extract_campaign_insights import extract_campaign_insights
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.transform_campaign_metadata import transform_campaign_metadata
from etl.load_campaign_insights import load_campaign_insights

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dags_google_ads(
    *,
    google_ads_client,
    customer_id: str,
    start_date: str,
    end_date: str,
):

    msg = (
        "🔁 [DAGS] Trigger to update Google Ads campaign insights with customer_id "
        f"{customer_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

    # Trigger Google Ads campaign insights DAG execution
    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60
    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    total_campaign_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        msg = (
            "🔁 [DAG] Trigger to extract Google Ads campaign insights from customer_id "
            f"{customer_id} for "
            f"{dags_split_date}..."
        )
        print(msg)
        logging.info(msg)

        for attempt in range(1, DAGS_MAX_ATTEMPTS + 1):
            try:
                insights = extract_campaign_insights(
                    google_ads_client=google_ads_client,
                    customer_id=customer_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if not insights:
                    msg = (
                        "⚠️ [DAG] No Google Ads campaign insights returned from customer_id "
                        f"{customer_id} then DAG execution "
                        f"{dags_split_date} will be skipped."
                    )
                    print(msg)
                    logging.warning(msg)
                    break

                daily_campaign_ids = {
                    row["campaign_id"] for row in insights
                }
                total_campaign_ids.update(daily_campaign_ids)

                load_campaign_insights(insights)

                break

            except Exception as e:
                retryable = getattr(e, "retryable", False)
                msg = (
                    f"⚠️ [DAGS] Failed to extract Google Ads campaign insights for {dags_split_date} in "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )
                print(msg)
                logging.warning(msg)

                if not retryable:
                    raise RuntimeError(
                        f"❌ [DAGS] Failed to extract Google Ads campaign insights for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be aborting."
                    ) from e

                if attempt == DAGS_MAX_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract Google Ads campaign insights for "
                        f"{dags_split_date} in "
                        f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to exceeded attempt limit then DAG execution will be aborting."
                    ) from e

                time.sleep(2 ** attempt)

        time.sleep(DAGS_MIN_COOLDOWN)
        date_cursor += timedelta(days=1)

    # Trigger Google Ads campaign metadata DAG execution
    if not total_campaign_ids:
        msg = (
            "⚠️ [DAGS] No Google Ads campaign_id appended for customer_id "
            f"{customer_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔁 [DAGS] Trigger to extracting Google Ads campaign metadata for "
        f"{len(total_campaign_ids)} campaign_id(s)..."
    )
    print(msg)
    logging.info(msg)

    metadata = extract_campaign_metadata(
        google_ads_client=google_ads_client,
        customer_id=customer_id,
        campaign_id_list=list(all_campaign_ids),
    )

    final_rows = transform_campaign_metadata(
        insights=None,
        metadata=metadata,
    )

    logging.info(
        f"[DAG] Google Ads DAG completed successfully | "
        f"campaigns={len(all_campaign_ids)}"
    )