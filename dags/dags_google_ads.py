
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
    google_ads_client: str,
    customer_id: str,
    start_date: str,
    end_date: str,
):
    """
    Google Ads Daily DAG (Sequential by Date)
    --------------------------------------------------
    - Ingest campaign insights day-by-day (sequential)
    - Each day is an atomic write
    - Retry & cooldown handled at DAG level
    - Campaign metadata extracted AFTER all days ingested
    --------------------------------------------------
    """

    logging.info(
        f"[DAG] Google Ads run started | "
        f"customer_id={customer_id} | "
        f"{start_date} → {end_date}"
    )

    # ------------------------------------------------------------------
    # 1. Init runtime context
    # ------------------------------------------------------------------
    credentials = load_google_ads_secret()
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")

    ingest_dates = (
        datetime.strptime(start_date, "%Y-%m-%d").date(),
        datetime.strptime(end_date, "%Y-%m-%d").date(),
    )

    date_cursor = ingest_dates[0]
    all_campaign_ids: set[str] = set()

    MAX_RETRY = 3
    API_COOLDOWN_SECONDS = 60

    # ------------------------------------------------------------------
    # 2. Ingest insights sequentially per day
    # ------------------------------------------------------------------
    while date_cursor <= ingest_dates[1]:
        ingest_date = date_cursor.strftime("%Y-%m-%d")

        logging.info(
            f"[DAG] Ingesting Google Ads campaign insights for {ingest_date}"
        )

        for attempt in range(1, MAX_RETRY + 1):
            try:
                insights = extract_campaign_insights(
                    google_ads_client=google_ads_client,
                    customer_id=customer_id,
                    start_date=ingest_date,
                    end_date=ingest_date,
                )

                if not insights:
                    logging.warning(
                        f"[DAG] No insights returned for {ingest_date}"
                    )
                    break

                # Collect campaign IDs
                daily_campaign_ids = {
                    row["campaign_id"] for row in insights
                }
                all_campaign_ids.update(daily_campaign_ids)

                logging.info(
                    f"[DAG] {ingest_date} | "
                    f"extracted {len(daily_campaign_ids)} campaign_id(s)"
                )

                # Load daily fact (BigQuery wrapper handles:
                # - mMMYYYY table
                # - delete overlap
                # - idempotency
                load_campaign_daily(insights)

                logging.info(
                    f"[DAG] {ingest_date} | load completed successfully"
                )
                break

            except Exception as e:
                logging.error(
                    f"[DAG] {ingest_date} | attempt {attempt}/{MAX_RETRY} failed: {e}"
                )
                if attempt == MAX_RETRY:
                    raise
                time.sleep(2 ** attempt)

        # Cooldown before next day
        time.sleep(API_COOLDOWN_SECONDS)
        date_cursor += timedelta(days=1)

    # ------------------------------------------------------------------
    # 3. Extract & load campaign metadata (after all days)
    # ------------------------------------------------------------------
    if not all_campaign_ids:
        logging.warning(
            "[DAG] No campaign_id collected, metadata extraction skipped"
        )
        return

    logging.info(
        f"[DAG] Extracting metadata for "
        f"{len(all_campaign_ids)} unique campaign_id(s)"
    )

    metadata = extract_campaign_metadata(
        google_ads_credentials=credentials,
        customer_id=customer_id,
        campaign_id_list=list(all_campaign_ids),
    )

    final_rows = transform_campaign_metadata(
        insights=None,  # or pass aggregated insight ref if needed
        metadata=metadata,
    )

    load_campaign_daily(final_rows)

    logging.info(
        f"[DAG] Google Ads run completed successfully | "
        f"campaigns={len(all_campaign_ids)}"
    )