import os
from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
import importlib
import logging
from zoneinfo import ZoneInfo

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
    raise ImportError("❌ [MAIN] Failed to import dags.dag_google_ads")

# ------------------------------------------------------------------
# SECRET MANAGER
# ------------------------------------------------------------------
from auth.internal_secret_manager import InternalGCPSecretManager
from google.ads.googleads.client import GoogleAdsClient

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    """
    Google Ads Main Entrypoint
    --------------------------------------------------
    Responsibilities:
        - Read & validate OS environment variables
        - Resolve MODE → (start_date, end_date) in GMT+7
        - Translate env → secret IDs
        - Load secrets from GCP Secret Manager
        - Initialize Google Ads client ONCE
        - Dispatch execution to Google Ads DAG
    --------------------------------------------------
    """

    # --------------------------------------------------
    # 1. Resolve time range (GMT+7 – HCM)
    # --------------------------------------------------
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

    # --------------------------------------------------
    # 2. Init Secret Manager
    # --------------------------------------------------
    secret_manager = InternalGCPSecretManager(project_id=PROJECT)

    # --------------------------------------------------
    # 3. Resolve secret IDs
    # --------------------------------------------------
    customer_id_secret = (
        f"{COMPANY}_secret_{DEPARTMENT}_google_account_id_{ACCOUNT}"
    )

    credentials_secret = (
        f"{COMPANY}_secret_all_google_token_access_user"
    )

    # --------------------------------------------------
    # 4. Load secrets
    # --------------------------------------------------
    customer_id = secret_manager.get_secret(customer_id_secret)["customer_id"]
    customer_id = customer_id.replace("-", "").strip()

    google_ads_credentials_dict = secret_manager.get_secret(
        credentials_secret
    )

    # --------------------------------------------------
    # 5. Init Google Ads client (ONCE)
    # --------------------------------------------------
    google_ads_client = GoogleAdsClient.load_from_dict(
        google_ads_credentials_dict
    )

    # --------------------------------------------------
    # 6. Execute DAG
    # --------------------------------------------------
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


# ------------------------------------------------------------------
# ENTRYPOINT
# ------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
