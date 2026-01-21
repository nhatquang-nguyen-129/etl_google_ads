from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
from datetime import datetime, timedelta
import logging
import time

from etl.extract_campaign_insights import extract_campaign_insights
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.transform_campaign_metadata import transform_campaign_metadata
from etl.load.bigquery import load_campaign_daily
from auth.secret_loader import load_google_ads_secret

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

    # ------------------------------------------------------------------
    # 1. Parse input dates (already normalized in MAIN)
    # ------------------------------------------------------------------
    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    all_campaign_ids: set[str] = set()

    DAGS_MAX_ATTEMPTS = 3
    DAGS_MIN_COOLDOWN = 60

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
                        f"[DAG] {ingest_date} | no insights returned"
                    )
                    logging.warning(
                        f"[DAG] {ingest_date} | no insights returned"
                    )
                    break

                daily_campaign_ids = {
                    row["campaign_id"] for row in insights
                }
                all_campaign_ids.update(daily_campaign_ids)

                load_campaign_daily(insights)

                logging.info(
                    f"[DAG] {ingest_date} | "
                    f"loaded {len(insights)} row(s) | "
                    f"{len(daily_campaign_ids)} campaign(s)"
                )
                break

            except Exception as e:
                msg = (
                    f"⚠️ [DAGS] Failed to extract Google Ads campaign insights for {ingest_date} in "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to "
                    f"{e}."
                )
                print(msg)
                logging.warning(msg)
                if attempt == DAGS_MAX_ATTEMPTS:
                    raise RuntimeError(
                    f"❌ [DAGS] Failed to extract Google Ads campaign insights for {ingest_date} in "
                    f"{attempt}/{DAGS_MAX_ATTEMPTS} attempt(s) due to exceeded attempt limit."
                )
                time.sleep(2 ** attempt)

        time.sleep(DAGS_MIN_COOLDOWN)
        date_cursor += timedelta(days=1)

    # ------------------------------------------------------------------
    # 3. Extract campaign metadata (once)
    # ------------------------------------------------------------------
    if not all_campaign_ids:
        logging.warning("[DAG] No campaign_ids collected → skip metadata")
        return

    logging.info(
        f"[DAG] Extracting metadata for {len(all_campaign_ids)} campaign(s)"
    )

    metadata = extract_campaign_metadata(
        google_ads_client=google_ads_client,
        customer_id=customer_id,
        campaign_id_list=list(all_campaign_ids),
    )

    final_rows = transform_campaign_metadata(
        insights=None,  # already loaded
        metadata=metadata,
    )

    logging.info(
        f"[DAG] Google Ads DAG completed successfully | "
        f"campaigns={len(all_campaign_ids)}"
    )