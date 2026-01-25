from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime
import json
import logging
import os
import argparse
from zoneinfo import ZoneInfo

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_google_ads import dags_google_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
]):
    raise EnvironmentError("❌ [TESTS] Failed to execute Google Ads manual entrypoint due to missing required environment variables.")

def main():
    """
    Backfill Google Ads entrypoint
    ---------
    Workflow
        1. Parse manual start_date / end_date
        2. Validate input date range
        3. Load secrets from GCP Secret Manager
        4. Initialize global Google Ads client
        5. Dispatch execution to DAGs orchestrator
    """

# CLI arguments parser for manual date range
    parser = argparse.ArgumentParser(description="Manual Google Ads ETL runner")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format"
    )
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError("❌ [TESTS] Failed to execute Google Ads main entrypoint due to start_date and end_date must be in YYYY-MM-DD format.")

    if start_date > end_date:
        raise ValueError("❌ [TESTS] Failed to execute Google Ads main entrypoint due to start_date must be less than or equal to end_date.")

    msg = (
        "🔄 [TESTS] Triggering to execute Google Ads main entrypoint for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company from "
        f"{start_date} to "
        f"{end_date} on Google Cloud Project "
        f"{PROJECT}..."
    )
    print(msg)
    logging.info(msg)

# Initialize Google Secret Manager
    try:
        msg = ("🔍 [TESTS] Initializing Google Secret Manager client...")
        print(msg)
        logging.info(msg)

        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        msg = ("✅ [TESTS] Successfully initialized Google Secret Manager client.")
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [TESTS] Failed to initialize Google Secret Manager client due to "
            f"{e}."
        )

# Resolve customer_id from Google Secret Manager
    try:
        secret_customer_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}"
        )
        secret_customer_name = (
            f"projects/{PROJECT}/secrets/{secret_customer_id}/versions/latest"
        )

        msg = (
            "🔍 [TESTS] Retrieving Google Ads customer_id from Secret Manager..."
        )
        print(msg)
        logging.info(msg)

        secret_customer_response = google_secret_client.access_secret_version(
            name=secret_customer_name,
            timeout=10.0,
        )

        google_customer_id = (
            secret_customer_response.payload.data.decode("utf-8")
            .replace("-", "")
            .replace(" ", "")
            .strip()
        )

        msg = (
            "✅ [TESTS] Successfully retrieved Google Ads customer_id "
            f"{google_customer_id}."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [TESTS] Failed to retrieve Google Ads customer_id due to "
            f"{e}."
        )

# Resolve credentials from Google Secret Manager
    try:
        secret_credentials_json = (
            f"{COMPANY}_secret_all_google_token_access_user"
        )
        secret_credentials_name = (
            f"projects/{PROJECT}/secrets/{secret_credentials_json}/versions/latest"
        )

        msg = (
            "🔍 [TESTS] Retrieving Google Ads credentials from Secret Manager..."
        )
        print(msg)
        logging.info(msg)

        secret_credentials_response = google_secret_client.access_secret_version(
            name=secret_credentials_name
        )

        google_ads_credentials = json.loads(
            secret_credentials_response.payload.data.decode("UTF-8")
        )

        msg = (
            "✅ [TESTS] Successfully retrieved Google Ads credentials."
        )
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [TESTS] Failed to retrieve Google Ads credentials due to "
            f"{e}."
        )

# Initialize global Google Ads client
    google_ads_config = {
        "developer_token": google_ads_credentials["developer_token"],
        "client_id": google_ads_credentials["client_id"],
        "client_secret": google_ads_credentials["client_secret"],
        "refresh_token": google_ads_credentials["refresh_token"],
        "login_customer_id": google_ads_credentials["login_customer_id"],
        "use_proto_plus": True,
    }

    try:
        msg = (
            "🔍 [TESTS] Initializing Google Ads client for customer_id "
            f"{google_customer_id}..."
        )
        print(msg)
        logging.info(msg)

        google_ads_client = GoogleAdsClient.load_from_dict(
            google_ads_config
        )

        msg = ("✅ [TESTS] Successfully initialized Google Ads client.")
        print(msg)
        logging.info(msg)

    except Exception as e:
        raise RuntimeError(
            "❌ [TESTS] Failed to initialize Google Ads client due to "
            f"{e}."
        )

# Execute DAGs
    dags_google_ads(
        google_ads_client=google_ads_client,
        customer_id=google_customer_id,
        start_date=start_date,
        end_date=end_date,
    )

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)