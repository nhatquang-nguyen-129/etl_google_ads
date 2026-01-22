import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
import subprocess
import os
import logging

def dbt_google_ads(
    *,
    google_cloud_project: str,
):
    """
    Run dbt Models for Google Ads Mart
    ---------
    Workflow:
        1. Initialize dbt execution environment (working directory, profiles, target)
        2. Trigger dbt build command for Google Ads models
        3. Capture dbt execution logs (stdout, stderr)
        4. Raise exception if dbt execution fails
    ---------
    Returns:
        1. subprocess.CompletedProcess:
            Contains dbt execution result including stdout, stderr, and return code
    """

    msg = (
        "🔁 [DBT] Running dbt build for Google Ads materialization to Google Cloud Project..."
        f"{google_cloud_project}..."
    )
    print(msg)
    logging.info(msg)

    DBT_PROJECT_DIR = ROOT_FOLDER_LOCATION / "dbt"
    DBT_PROFILES_DIR = DBT_PROJECT_DIR / "profiles"

    cmd = [
        "dbt",
        "build",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--select", "tag:mart",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env={
                **os.environ,
                "GOOGLE_CLOUD_PROJECT": google_cloud_project,
            },
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


