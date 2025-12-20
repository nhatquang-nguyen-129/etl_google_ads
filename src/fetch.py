"""
==================================================================
GOOGLE FETCHING MODULE
------------------------------------------------------------------
This module handles authenticated data retrieval from the Google 
Marketing API, consolidating all campaign, ad, creative, and metadata 
fetching logic into a unified, maintainable structure for ingestion.

It ensures reliable access to Google Ads data with controlled rate 
limits, standardized field mapping, and structured outputs for 
downstream enrichment and transformation stages.

✔️ Initializes secure Google SDK sessions and retrieves credentials  
✔️ Fetches campaign, ad, and creative data via authenticated API calls  
✔️ Handles pagination, rate limiting and error retries automatically  
✔️ Returns normalized and schema-ready DataFrames for processing  
✔️ Logs detailed runtime information for monitoring and debugging  

⚠️ This module focuses solely on data retrieval and extraction.  
It does not perform schema enforcement, data enrichment, or 
storage operations such as uploading to BigQuery.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integraton
import logging

# Add Python JSON ultilities for integration
import json

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Ads modules for integration
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal Google Ads modules for handling
from src.schema import enforce_table_schema

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. FETCH GOOGLE ADS METADATA

# 1.1. Fetch Google Ads campaign metadata
def fetch_campaign_metadata(fetch_campaign_ids: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
    
    # 1.1.1. Start timing the Google Ads campaign metadata fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch Google Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Google Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    
    try:

    # 1.1.2 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")          
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 

    # 1.1.3. Get Google Ads customer_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get Google Ads customer_id from Google Secret Manager"
        fetch_section_start = time.time()           
        try: 
            print(f"🔍 [FETCH] Retrieving Google Ads customer_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads customer_id for account {ACCOUNT} from Google Secret Manager...")
            google_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            google_secret_name = f"projects/{PROJECT}/secrets/{google_secret_id}/versions/latest" 
            response = google_secret_client.access_secret_version(request={"name": google_secret_name})
            creds = json.loads(response.payload.data.decode("utf-8"))
            customer_id = creds["customer_id"]
            print(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {customer_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {customer_id} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    try:

    
    # 1.1.3. Initialize Google Ads client
        try:
            print(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            logging.info(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            google_ads_client = GoogleAdsClient.load_from_dict(creds, version="v16")
            print(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Ads client due to unexpected error {e}.") from e

    # 1.1.4. Make Google Ads API call for campaign metadata
        fetch_section_name = "[FETCH] Make Google Ads API call for campaign metadata"
        fetch_section_start = time.time()           
        try:
            fetch_fields = [
                "campaign.id",
                "campaign.name",
                "campaign.status",
                "campaign.advertising_channel_type",
                "campaign.advertising_channel_sub_type",
                "campaign.serving_status",
                "campaign.start_date",
                "campaign.end_date"
            ]
            all_records = []            
            print(f"🔍 [FETCH] Retrieving campaign metadata for {len(campaign_id_list)} Google Ads campaign_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving campaign metadata for {len(campaign_id_list)} Google Ads campaign_id(s)...")
            ids_str = ",".join(map(str, campaign_id_list))
            query = f"SELECT {', '.join(fetch_fields)} FROM campaign WHERE campaign.id IN ({ids_str})"
            google_ads_service = google_ads_client.get_service("GoogleAdsService")
            fetch_campaign_response = google_ads_service.search(customer_id=customer_id, query=query)
            for row in fetch_campaign_response:
                record = {
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                    "serving_status": row.campaign.serving_status.name,
                    "start_date": row.campaign.start_date,
                    "end_date": row.campaign.end_date,
                    "account_id": customer_id,
                }
                all_records.append(record)
        except Exception as e:
            print(f"❌ [FETCH] Failed to campaign metadata for Google Ads due to {e}.")
            logging.error(f"❌ [FETCH] Failed to campaign metadata for Google Ads due to {e}.")
            return pd.DataFrame()

    # 1.1.5. Convert to Python DataFrame
        if not all_records:
            print("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            logging.warning("⚠️ [FETCH] No Google Ads campaign metadata fetched.")
            return pd.DataFrame()
        try:
            print(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} campaign_id(s) to DataFrame...")
            logging.info(f"🔄 [FETCH] Converting metadata for {len(campaign_id_list)} campaign_id(s) to DataFrame...")
            df = pd.DataFrame(all_records)
            print(f"✅ [FETCH] Successfully converted metadata to DataFrame with {len(df)} row(s).")
            logging.info(f"✅ [FETCH] Successfully converted metadata to DataFrame with {len(df)} row(s).")
        except Exception as e:
            print(f"❌ [FETCH] Failed to convert metadata to DataFrame: {e}")
            logging.error(f"❌ [FETCH] Failed to convert metadata to DataFrame: {e}")
            return pd.DataFrame()
        return df
    except Exception as e:
        print(f"❌ [FETCH] Unexpected error: {e}")
        logging.error(f"❌ [FETCH] Unexpected error: {e}")
        return pd.DataFrame()

# 2. FETCH GOOGLE ADS INSIGHTS

# 2.1. Fetch campaign insights for Google Ads
def fetch_campaign_insights(customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {start_date} to {end_date}...")

    try:
    # 2.1.1. Initialize Google Secret Manager client
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client because secret not found.") from e
        except GoogleAPICallError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Secret Manager client due to API call error.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Secret Manager client due to unexpected error {e}.") from e
    
    # 2.1.2. Prepare Google Secret Manager id(s)
        print(f"🔍 [FETCH] Retrieving Google Ads ad account information for {ACCOUNT} from Google Secret Manager...")
        logging.info(f"🔍 [FETCH] Retrieving Google Ads ad account information for {ACCOUNT} from Google Secret Manager...") 
        google_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
        google_secret_name = f"projects/{PROJECT}/secrets/{google_secret_id}/versions/latest"
        response = google_secret_client.access_secret_version(request={"name": google_secret_name})
        creds = json.loads(response.payload.data.decode("utf-8"))
        print(f"✅ [FETCH] Successfully retrieved Google Ads account secret_id {google_secret_id} for account environment variable {ACCOUNT} from Google Secret Manager.")
        logging.info(f"✅ [FETCH] Successfully retrieved Google Ads account secret_id {google_secret_id} for account environment variable {ACCOUNT} from Google Secret Manager.")   
    
    # 2.1.2. Initialize Google Ads client
        try:
            print(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            logging.info(f"🔍 [FETCH] Initializing Google Ads client for customer_id {customer_id} from Google Secret Manager account secret_id {google_secret_id}...")
            google_ads_client = GoogleAdsClient.load_from_dict(creds, version="v16")
            print(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Ads client for customer_id {customer_id}.")
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Ads client due to credentials error.") from e
        except PermissionDenied as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Ads client due to permission denial.") from e
        except NotFound as e:
            raise RuntimeError("❌ [FETCH] Failed to initialize Google Ads client because a resource not found.") from e
        except GoogleAdsException as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Ads client due to {e.error.code} when API responded.") from e
        except GoogleAPICallError as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Ads client due to {e} when API not responed.") from e
        except Exception as e:
            raise RuntimeError(f"❌ [FETCH] Failed to initialize Google Ads client due to unexpected error {e}.") from e

    # 2.1.3. Define parameter(s) and field(s)
        query = f"""
            SELECT
                campaign.id,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                segments.date
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        print(f"🔍 [FETCH] Preparing Google Ads API query {query}...")
        logging.info(f"🔍 [FETCH] Preparing Google Ads API query {query}...")
        google_ads_service = google_ads_client.get_service("GoogleAdsService")

    # 2.1.4. Make Google Ads API call
        records = []
        for attempt in range(2):
            try:
                print(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")
                logging.info(f"🔍 [FETCH] Fetching campaign insights for customer_id {customer_id}, attempt {attempt+1}...")

                response = google_ads_service.search(customer_id=customer_id, query=query)

                for row in response:
                    records.append({
                        "campaign_id": row.campaign.id,
                        "date": row.segments.date,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "conversions": row.metrics.conversions,
                        "cost_micros": row.metrics.cost_micros,
                    })

                if not records:
                    print("⚠️ [FETCH] No data returned from Google Ads API.")
                    logging.warning("⚠️ [FETCH] No data returned from Google Ads API.")
                    return pd.DataFrame()

                df = pd.DataFrame(records)
                print(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) from Google Ads.")
                logging.info(f"✅ [FETCH] Successfully retrieved {len(df)} row(s) from Google Ads.")
                return df

            except Exception as e_inner:
                print(f"⚠️ [FETCH] Google Ads API error: {e_inner}")
                logging.error(f"⚠️ [FETCH] Google Ads API error: {e_inner}")
                if attempt == 1:
                    return pd.DataFrame()
                time.sleep(1)

    except Exception as e_outer:
        print(f"❌ [FETCH] Failed to fetch Google Ads campaign insights: {e_outer}")
        logging.error(f"❌ [FETCH] Failed to fetch Google Ads campaign insights: {e_outer}")
        return pd.DataFrame()