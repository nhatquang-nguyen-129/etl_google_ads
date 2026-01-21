import os
from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import json
import importlib
import logging
from zoneinfo import ZoneInfo

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import secretmanager

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
    MODE
]):
    raise EnvironmentError("❌ [MAIN] Failed to execute Google Ads main entrypoint due to missing required environment variables.")

try:
    dag_google_ads = importlib.import_module("dags.dag_google_ads")
except ModuleNotFoundError:
    raise ImportError("❌ [MAIN] Failed to execute Google Ads main entrypoint due todags.dag_google_ads cannot be imported.")

def main():
    """
    Google Ads entrypoint
    ---------
    Main is responsible for preparing the entire execution environment:
        - Resolve execution time window from MODE
        - Read & validate OS environment variables
        - Load secrets from GCP Secret Manager
        - Initialize Google Ads client exactly once
        - Dispatch execution to DAG orchestrator

    DAGs must NOT initialize clients or read secrets.
    DAGs only coordinate execution order and retries.
    """

# Resolve input time range
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    today = datetime.now(ICT)

    if MODE == "today":
        start_date = end_date = today.strftime("%Y-%m-%d")

    elif MODE == "last3days":
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "last7days":
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "thismonth":
        start_date = today.replace(day=1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

    elif MODE == "lastmonth":
        last_month_end = today.replace(day=1) - timedelta(days=1)
        start_date = last_month_end.replace(day=1).strftime("%Y-%m-%d")
        end_date = last_month_end.strftime("%Y-%m-%d")

    else:
        raise ValueError(f"⚠️ [MAIN] Unsupported MODE='{MODE}'")

# Resolve input from Google Secret Manager
    google_secret_client = secretmanager.SecretManagerServiceClient()
    
    secret_customer_id = (
        f"{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}"
    )
    secret_customer_name = (
        f"projects/{PROJECT}/secrets/{secret_customer_id}/versions/latest"
    )
    secret_customer_response = google_secret_client.access_secret_version(
        name=secret_customer_name
    )
    google_customer_id = (
        json.loads(secret_customer_response.payload.data.decode("UTF-8"))["customer_id"]
        .replace("-", "")
        .strip()
    )

    secret_credentials_json = (
        f"{COMPANY}_secret_all_google_token_access_user"
    )
    secret_credentials_name = (
        f"projects/{PROJECT}/secrets/{secret_credentials_json}/versions/latest"
    )
    secret_credentials_response = google_secret_client.access_secret_version(
        name=secret_credentials_name
    )
    google_ads_credentials = json.loads(
        secret_credentials_response.payload.data.decode("UTF-8")
    )

    logging.info("🔐 [MAIN] Google Ads secrets loaded")

# Initialize global Google Ads client
    google_ads_config = {
        "developer_token": google_ads_credentials["developer_token"],
        "client_id": google_ads_credentials["client_id"],
        "client_secret": google_ads_credentials["client_secret"],
        "refresh_token": google_ads_credentials["refresh_token"],
        "login_customer_id": google_ads_credentials["login_customer_id"],
        "use_proto_plus": True,
    }

    google_ads_client = GoogleAdsClient.load_from_dict(
        google_ads_config
    )

    msg = (
        "✅ [MAIN] Successfully initialized global Google Ads client for customer_id "
        f"{google_customer_id}."
    )
    print(msg)
    logging.info(msg)

# Execute DAGS
    try:
        logging.info(
            f"[MAIN] Trigger Google Ads DAG | "
            f"company={COMPANY} | "
            f"customer_id={customer_id} | "
            f"{start_date} → {end_date}"
        )

        print(
            f"🚀 [MAIN] Google Ads run | "
            f"customer_id={customer_id} | "
            f"{start_date} → {end_date}"
        )

        dag_google_ads.dag__(
            google_ads_client=google_ads_client,
            customer_id=customer_id,
            start_date=start_date,
            end_date=end_date
        )

        logging.info("[MAIN] Google Ads run completed successfully")

    except Exception as e:
        logging.error(f"❌ [MAIN] Google Ads run failed: {e}")
        raise

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)