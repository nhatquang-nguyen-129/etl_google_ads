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
            customer_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            customer_secret_name = f"projects/{PROJECT}/secrets/{customer_secret_id}/versions/latest"
            customer_secret_response = google_secret_client.access_secret_version(request={"name": customer_secret_name})
            fetch_customer_id = customer_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {fetch_customer_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {fetch_customer_id} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads customer_id from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads customer_id from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.4. Get Google Ads OAuth2 credential from Google Secret Manager
        fetch_section_name = "[FETCH] Get Google Ads OAuth2 credential from Google Secret Manager"
        fetch_section_start = time.time()           
        try: 
            print(f"🔍 [FETCH] Retrieving Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager...")
            credential_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_credential_access_user"
            credential_secret_name = f"projects/{PROJECT}/secrets/{credential_secret_id}/versions/latest" 
            crendential_secret_response = google_secret_client.access_secret_version(request={"name": credential_secret_name})
            fetch_access_user = json.loads(crendential_secret_response.payload.data.decode("utf-8"))
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads OAuth2 credentials from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads OAuth2 credentials from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.1.5. Initialize Google Ads client from OAuth2 credentials
        fetch_section_name = "[FETCH] Initialize Google Ads client from OAuth2 credentials"
        fetch_section_start = time.time()
        try:            
            print(f"🔍 [FETCH] Initializing Google Ads client for {ACCOUNT} with OAuth2 credentials...")
            logging.info(f"🔍 [FETCH] Initializing Google Ads client for {ACCOUNT} with OAuth2 credentials...")
            google_ads_client = GoogleAdsClient.load_from_dict(
                fetch_access_user, 
                version="v22"
            )
            google_ads_service = google_ads_client.get_service("GoogleAdsService")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Ads client for account {ACCOUNT} with OAuth2 credentials.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Ads client for account {ACCOUNT} with OAuth2 credentials.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Ads client with OAuth2 credentials due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Ads client with OAuth2 credentials due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)     

    # 1.1.6. Make Google Ads API call for ad account infomation
        fetch_section_name = "[FETCH] Make Google Ads API call for ad account infomation"
        fetch_section_start = time.time()        
        try:
            query_select_config = """
                SELECT
                    customer.descriptive_name
                FROM customer
                LIMIT 1
            """
            print(f"🔍 [FETCH] Retrieving Google Ads customer_descriptive_name for customer_id {fetch_customer_id}...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads customer_descriptive_name for customer_id {fetch_customer_id}...")            
            fetch_customer_responses = google_ads_service.search(
                customer_id=fetch_customer_id,
                query=query_select_config
            )
            for fetch_customer_response in fetch_customer_responses:
                fetch_customer_name = fetch_customer_response.customer.descriptive_name
                break
            if fetch_customer_name:
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved Google Ads customer_descriptive_name {fetch_customer_name} for customer_id {fetch_customer_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved Google Ads customer_descriptive_name {fetch_customer_name} for customer_id {fetch_customer_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve Google Ads customer_descriptive_name due to data not found.")
                logging.error(f"❌ [FETCH] Failed to retrieve Google Ads customer_descriptive_name due to data not found.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads customer_descriptive_name due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads customer_descriptive_name due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.1.7. Make Google Ads API call for campaign metadata
        fetch_section_name = "[FETCH] Make Google Ads API call for campaign metadata"
        fetch_section_start = time.time()           
        try:
            fetch_campaign_metadatas = []            
            fetch_campaign_fields = [
                "campaign.id",
                "campaign.name",
                "campaign.status",
                "campaign.serving_status"
            ]
            query_select_config = f"""
                SELECT
                    {', '.join(fetch_campaign_fields)}
                FROM campaign
                WHERE campaign.id IN ({', '.join(map(str, fetch_campaign_ids))})
            """     
            print(f"🔍 [FETCH] Retrieving Google Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
            fetch_campaign_responses = google_ads_service.search(
                customer_id=fetch_customer_id, 
                query=query_select_config
            )
            for fetch_campaign_response in fetch_campaign_responses:
                fetch_campaign_metadata = {
                    "campaign_id": fetch_campaign_response.campaign.id,
                    "campaign_name": fetch_campaign_response.campaign.name,
                    "campaign_status_name": fetch_campaign_response.campaign.status.name,
                    "customer_id": fetch_customer_id,
                    "customer_descriptive_name": fetch_customer_name,
                }
                fetch_campaign_metadatas.append(fetch_campaign_metadata)
            fetch_df_flattened = pd.DataFrame(fetch_campaign_metadatas)
            if len(fetch_campaign_metadatas) == len(fetch_campaign_ids):
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
            elif len(fetch_campaign_ids) > 0 and len(fetch_campaign_metadatas) < len(fetch_campaign_ids):
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially retrieved Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
                logging.warning(f"⚠️ [FETCH] Partially retrieved Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
                logging.error(f"❌ [FETCH] Failed to retrieve Google Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for customer_id {fetch_customer_id}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.8. Trigger to enforce schema for Google Ads campaign metadata
        fetch_section_name = "[FETCH] Trigger to enforce schema for Google Ads campaign metadata"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for Google Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for Google Ads campaign metadata with {len(fetch_df_flattened)} row(s)...")            
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_metadata")            
            fetch_df_enforced = fetch_results_schema["schema_df_final"]
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]            
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger Google Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.1.9. Summarize fetch results for Google Ads campaign metadata
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_input = len(fetch_campaign_ids)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"
            print(f"❌ [FETCH] Failed to complete Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_rows_output == fetch_rows_input:
            fetch_status_final = "fetch_succeed_all"
            print(f"🏆 [FETCH] Successfully completed Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")            
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed Google Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")         
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_input": fetch_rows_input, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 2. FETCH GOOGLE ADS INSIGHTS

# 2.1. Fetch campaign insights for Google Ads
def fetch_campaign_insights(fetch_date_start: str, fetch_date_end: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
    logging.info(f"🚀 [FETCH] Starting to fetch Google Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")    

    # 2.1.1. Start timing the Google Ads campaign insights fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.1.2. Initialize Google Secret Manager client
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
    
    # 2.1.3. Get Google Ads customer_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get Google Ads customer_id from Google Secret Manager"
        fetch_section_start = time.time()         
        try:
            print(f"🔍 [FETCH] Retrieving Google Ads customer_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads customer_id for account {ACCOUNT} from Google Secret Manager...")
            customer_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_account_id_{ACCOUNT}"
            customer_secret_name = f"projects/{PROJECT}/secrets/{customer_secret_id}/versions/latest"
            customer_secret_response = google_secret_client.access_secret_version(request={"name": customer_secret_name})
            fetch_customer_id = customer_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {fetch_customer_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads customer_id {fetch_customer_id} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads customer_id from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads customer_id from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 2.1.4. Get Google Ads OAuth2 credential from Google Secret Manager
        fetch_section_name = "[FETCH] Get Google Ads OAuth2 credential from Google Secret Manager"
        fetch_section_start = time.time()           
        try: 
            print(f"🔍 [FETCH] Retrieving Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager...")
            credential_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_{PLATFORM}_credential_access_user"
            credential_secret_name = f"projects/{PROJECT}/secrets/{credential_secret_id}/versions/latest" 
            crendential_secret_response = google_secret_client.access_secret_version(request={"name": credential_secret_name})
            fetch_access_user = json.loads(crendential_secret_response.payload.data.decode("utf-8"))
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved Google Ads OAuth2 credentials for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve Google Ads OAuth2 credentials from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve Google Ads OAuth2 credentials from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 2.1.5. Initialize Google Ads client from OAuth2 credentials
        fetch_section_name = "[FETCH] Initialize Google Ads client from OAuth2 credentials"
        fetch_section_start = time.time()
        try:            
            print(f"🔍 [FETCH] Initializing Google Ads client for {ACCOUNT} with OAuth2 credentials...")
            logging.info(f"🔍 [FETCH] Initializing Google Ads client for {ACCOUNT} with OAuth2 credentials...")
            google_ads_client = GoogleAdsClient.load_from_dict(
                fetch_access_user, 
                version="v22"
            )
            google_ads_service = google_ads_client.get_service("GoogleAdsService")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Ads client for account {ACCOUNT} with OAuth2 credentials.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Ads client for account {ACCOUNT} with OAuth2 credentials.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Ads client with OAuth2 credentials due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Ads client with OAuth2 credentials due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)    

    # 2.1.6. Make Google Ads API call for campaign insights
        fetch_section_name = "[FETCH] Make Google Ads API call for campaign insights"
        fetch_section_start = time.time()
        try:
            fetch_campaign_insights = []
            fetch_attempts_queued = 3            
            fetch_campaign_fields ={
                "campaign.id",
                "metrics.impressions",
                "metrics.clicks",
                "metrics.conversions",
                "metrics.cost_micros",
                "segments.date"
            }
            query_select_config = f"""
                SELECT
                    {', '.join(fetch_campaign_fields)}
                FROM campaign
                WHERE segments.date BETWEEN '{fetch_date_start}' AND '{fetch_date_end}'
            """    
            for fetch_attempt_queued in range(fetch_attempts_queued):
                try:
                    print(f"🔍 [FETCH] Retrieving Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} with attempt {fetch_attempt_queued + 1}/{fetch_attempts_queued}...")
                    logging.info(f"🔍 [FETCH] Retrieving Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} with attempt {fetch_attempt_queued + 1}/{fetch_attempts_queued}...")
                    fetch_campaign_responses = google_ads_service.search(
                        customer_id=fetch_customer_id, 
                        query=query_select_config
                    )
                    for fetch_campaign_response in fetch_campaign_responses:
                        fetch_campaign_insights.append({
                            "customer_id": fetch_customer_id,
                            "campaign_id": fetch_campaign_response.campaign.id,
                            "date": fetch_campaign_response.segments.date,
                            "impressions": fetch_campaign_response.metrics.impressions,
                            "clicks": fetch_campaign_response.metrics.clicks,
                            "conversions": fetch_campaign_response.metrics.conversions,
                            "cost_micros": fetch_campaign_response.metrics.cost_micros,
                        })
                    fetch_df_flattened = pd.DataFrame(fetch_campaign_insights)
                    fetch_sections_status[fetch_section_name] = "succeed"
                    print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of Google Ads campaign insights.")
                    logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of Google Ads campaign insights.")
                    break
                except Exception as e:
                    if fetch_attempt_queued < fetch_attempts_queued - 1:
                        fetch_attempt_delayed = 60 + (fetch_attempt_queued * 60)
                        print(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve Google Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
                        logging.warning(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve Google Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
                        time.sleep(fetch_attempt_delayed)                    
                    else:
                        fetch_sections_status[fetch_section_name] = "failed"
                        print(f"❌ [FETCH] Failed to retrieve Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
                        logging.error(f"❌ [FETCH] Failed to retrieve Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
        finally:
            fetch_cooldown_queued = 60 + 30 * max(0, fetch_attempt_queued)
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.7. Trigger to enforce schema for Google Ads campaign insights
        fetch_section_name = "[FETCH] Trigger to enforce schema for Google Ads campaign insights"
        fetch_section_start = time.time()        
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for Google Ads campaign insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_insights")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger Google Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.7. Summarize fetch results for Google Ads campaign insights
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_days_input = ((pd.to_datetime(fetch_date_end) - pd.to_datetime(fetch_date_start)).days + 1)
        fetch_days_output = (fetch_df_final["stat_time_day"].nunique() if not fetch_df_final.empty and "stat_time_day" in fetch_df_final.columns else 0)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }        
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"            
            print(f"❌ [FETCH] Failed to complete Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_days_output == fetch_days_input:
            fetch_status_final = "fetch_succeed_all" 
            print(f"🏆 [FETCH] Successfully completed Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed Google Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")            
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed,
                "fetch_cooldown_queued": fetch_cooldown_queued,                
                "fetch_days_input": fetch_days_input,
                "fetch_days_output": fetch_days_output,
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded,
                "fetch_sections_failed": fetch_sections_failed,
                "fetch_sections_detail": fetch_sections_detail,
                "fetch_rows_output": fetch_rows_output,
            },
        }
    return fetch_results_final