import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import subprocess
import os
import logging
import json

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dbt_google_ads(
    *,
    google_cloud_project: str,
):
    """
    Run dbt Models for Google Ads Mart
    ---------
    Workflow:
        1. Initialize dbt execution environment (directory, profiles, target)
        2. Trigger dbt build command for Google Ads models
        3. Capture dbt execution logs with stdout and stderr
        4. Raise exception if dbt execution fails
    ---------
    Returns:
        1. subprocess.CompletedProcess:
            Contains dbt execution result including stdout, stderr, and return code
    """

    msg = (
        "🔁 [DBT] Running dbt build for Google Ads materialization to Google Cloud Project "
        f"{google_cloud_project}..."
    )
    print(msg)
    logging.info(msg)

    # ===============================
    # INLINE ENV → DBT CONFIG
    # ===============================
    os.environ["PROJECT"] = google_cloud_project
    os.environ["DBT_DATASET"] = f"{COMPANY}_dataset_google_api_mart"

    # vars for dbt_project.yml
    os.environ["COMPANY"] = COMPANY
    os.environ["DEPARTMENT"] = DEPARTMENT
    os.environ["ACCOUNT"] = ACCOUNT

    cmd = [
        "dbt",
        "build",
        "--project-dir", "dbt",
        "--profiles-dir", "dbt",
        "--select", "tag:mart",
    ]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            env={**os.environ},
            text=True,
        )

        print(result.stdout)
        logging.info(result.stdout)
        
        msg = (
            "✅ [DBT] Successfully completed dbt build for Google Ads to Google Cloud Project "
            f"{google_cloud_project}."
        )
        print(msg)
        logging.info(msg)

    except subprocess.CalledProcessError as e:
        print(e.stdout)
        logging.error(e.stdout)
        print(e.stderr)
        logging.error(e.stderr)       
        raise RuntimeError(
            "❌ [DBT] Failed to complete dbt build for Google Ads to Google Cloud Project"
            f"{google_cloud_project} due to "
            f"{e}."
        )
