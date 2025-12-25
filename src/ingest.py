"""
==================================================================
GOOGLE INGESTION MODULE
------------------------------------------------------------------
This module ingests raw data from the Google Ads fetching module 
into Google BigQuery, establishing the foundational raw layer used 
for centralized storage and historical retention.

It manages the complete ingestion flow — from authentication to 
data fetching, schema validation and loading into Google BigQuery 
tables segmented by campaign, ad, creative and metadata.

✔️ Supports both append and truncate modes via write_disposition
✔️ Validates data structure using centralized schema utilities
✔️ Applies lightweight normalization required for raw-layer loading
✔️ Implements granular logging and CSV-based error traceability
✔️ Ensures pipeline reliability through retry and checkpoint logic

⚠️ This module is dedicated solely to raw-layer ingestion.  
It does not handle advanced transformations, metric modeling, 
or aggregated data processing beyond the ingestion boundary.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities forintegration
import logging

# Add Python time ultilities for integration
import time

# Add Python UUID ultilities for integration
import uuid

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google API core modules for integration
from google.api_core.exceptions import NotFound

# Add Google Cloud modules for integration
from google.cloud import bigquery

# Add internal Google modules for handling
from src.enrich import (
    enrich_campaign_insights, 
    enrich_ad_insights   
)
from src.fetch import (
    fetch_account_name,
    fetch_campaign_metadata, 
    fetch_adset_metadata,
    fetch_ad_metadata,
    fetch_ad_creative,
    fetch_campaign_insights, 
    fetch_ad_insights,
)
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

# 1. INGEST GOOGLE ADS METADATA

# 1.1. Ingest campaign metadata for Google Ads
def ingest_campaign_metadata(ingest_campaign_ids: list) -> pd.DataFrame:
    print(f"🚀 [INGEST] Starting to ingest Google Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
    logging.info(f"🚀 [INGEST] Starting to ingest Google Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")

    # 1.1.1. Start timing Google Ads campaign metadata ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    print(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest TikTok Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Trigger to fetch Google Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to fetch Google Ads campaign metadata"
        ingest_section_start = time.time()
        
        try:
            print(f"🔁 [INGEST] Triggering to fetch Google Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
            logging.info(f"🔁 [INGEST] Triggering to fetch Google Ads campaign metadata for {len(ingest_campaign_ids)} campaign_id(s)...")
            ingest_results_fetched = fetch_campaign_metadata(fetch_campaign_ids=ingest_campaign_ids)
            ingest_df_fetched = ingest_results_fetched["fetch_df_final"]            
            ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
            ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
            if ingest_status_fetched == "fetch_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")               
            elif ingest_status_fetched == "fetch_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [INGEST] Partially triggered Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.warning(f"⚠️ [INGEST] Partially triggered Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata fetching with {ingest_summary_fetched['fetch_rows_output']}/{ingest_summary_fetched['fetch_rows_input']} fetched row(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.3. Trigger to enforce schema for Google Ads campaign metadata
        ingest_section_name = "[INGEST] Trigger to enforce schema for Google Ads campaign metadata"
        ingest_section_start = time.time()
        
        try:
            print(f"🔄 [INGEST] Triggering to enforce schema for Google Ads campaign metadata with {len(ingest_df_fetched)} fetched row(s)...")
            logging.info(f"🔄 [INGEST] Triggering to enforce schema for Google Ads campaign metadata with {len(ingest_df_fetched)} fetched row(s)...")
            ingest_results_enforced = enforce_table_schema(ingest_df_fetched, "ingest_campaign_metadata")
            ingest_df_enforced = ingest_results_enforced["schema_df_final"]
            ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
            ingest_status_enforced = ingest_results_enforced["schema_status_final"]                
            if ingest_status_enforced == "schema_succeed_all":
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully triggered Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [INGEST] Successfully triggered Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            elif ingest_status_enforced == "schema_succeed_partial":
                ingest_sections_status[ingest_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            else:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [INGEST] Failed to trigger Google Ads campaign metadata schema enforcement with {ingest_summary_enforced['schema_rows_output']}/{len(ingest_df_fetched)} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.4. Prepare Google BigQuery table_id for ingestion
        ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
        ingest_section_start = time.time()
        
        try:
            raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
            raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
            ingest_sections_status[ingest_section_name] = "succeed"   
            print(f"🔍 [INGEST] Preparing to ingest Google Ads campaign metadata for {len(ingest_df_fetched)} enforced row(s) to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Preparing to ingest Google Ads campaign metadata for {len(ingest_df_fetched)} enforced row(s) to Google BigQuery table {raw_table_campaign}...")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.5. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.6. Delete existing rows or create new table if not exist
        ingest_section_name = "[INGEST] Delete existing rows or create new table if not exist"
        ingest_section_start = time.time()
        
        try:
            
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            
            try:
                print(f"🔍 [INGEST] Checking Google Ads Ads campaign metadata table {raw_table_campaign} existence...")
                logging.info(f"🔍 [INGEST] Checking Google Ads campaign metadata table {raw_table_campaign} existence...")
                google_bigquery_client.get_table(raw_table_campaign)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Google Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Google Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
            
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Google Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Google Ads campaign metadata table {raw_table_campaign} not found then table creation will be proceeding...")
        
        # Defined table creation               
                table_schemas_defined = []
                table_clusters_defined = []
                table_partition_defined = []      

        # Config table creation
                table_schemas_config = (
                    table_schemas_defined
                    if table_schemas_defined
                    else [
                        bigquery.SchemaField(
                            col,
                            "INT64" if dtype.name.startswith("int")
                            else "FLOAT64" if dtype.name.startswith("float")
                            else "BOOL" if dtype.name == "bool"
                            else "TIMESTAMP" if "datetime" in dtype.name
                            else "STRING"
                        )
                        for col, dtype in ingest_df_deduplicated.dtypes.items()
                    ]
                )
            
                table_partition_config = (
                    table_partition_defined
                    if table_partition_defined in ingest_df_deduplicated.columns
                    else None
                )
        
                table_clusters_config = (
                    [col for col in table_clusters_defined if col in ingest_df_deduplicated.columns]
                    if table_clusters_defined
                    else None
                )
        
        # Execute table creation                
                try:    
                    print(f"🔍 [INGEST] Creating Google Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_config} and cluster on {table_clusters_config}...")
                    logging.info(f"🔍 [INGEST] Creating Google Ads campaign metadata table defined name {raw_table_campaign} with partition on {table_partition_config} and cluster on {table_clusters_config}...")
                    create_table_config = bigquery.Table(
                        raw_table_campaign,
                        schema=table_schemas_config
                    )
                    if table_partition_config:
                        create_table_config.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_config
                        )
                    if table_clusters_config:
                        create_table_config.clustering_fields = table_clusters_config
                    create_table_execute = google_bigquery_client.create_table(create_table_config)
                    create_table_id = create_table_execute.full_table_id
                    print(f"✅ [INGEST] Successfully created Google Ads campaign metadata table actual name {create_table_id} with partition on {table_partition_config} and cluster on {table_clusters_config}.")
                    logging.info(f"✅ [INGEST] Successfully created Google Ads campaign metadata table actual name {create_table_id} with partition on {table_partition_config} and cluster on {table_clusters_config}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Google Ads campaign metadata table {raw_table_campaign} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Google Ads campaign metadata table {raw_table_campaign} due to {e}.")
            
            else:
                print(f"🔄 [INGEST] Found Google Ads campaign metadata table {raw_table_campaign} then existing rows deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found Google Ads campaign metadata table {raw_table_campaign} then existing rows deletion will be proceeding...")
        
        # Define batch deletion              
                unique_keys_defined = [
                    "customer_id", 
                    "campaign_id"
                ]                
        
            # Config batch deletion
                unique_keys_config = [
                    unique_key_defined
                    for unique_key_defined in unique_keys_defined
                    if unique_key_defined in ingest_df_deduplicated.columns
                ]                
        
                if len(unique_keys_config) == 1:
                    unique_keys_value = (
                        ingest_df_deduplicated[unique_keys_config[0]]
                        .dropna()
                        .astype(str)
                        .unique()
                        .tolist()
                    )

                else:
                    unique_keys_value = (
                        ingest_df_deduplicated[unique_keys_config]
                        .dropna()
                        .astype(str)
                        .drop_duplicates()
                        .apply(tuple, axis=1)
                        .tolist()
                    )

        # Execute batch deletion
                try:
                    print(f"🔍 [INGEST] Creating temporary table contains duplicated Google Ads campaign metadata unique keys {unique_keys_config} for batch deletion...")
                    logging.info(f"🔍 [INGEST] Creating temporary table contains duplicated Google Ads campaign metadata unique keys {unique_keys_config} for batch deletion...")
                    temporary_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                    temporary_df_value = pd.DataFrame(
                        unique_keys_value,
                        columns=unique_keys_config
                    )                    
                    load_table_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    load_table_execute = google_bigquery_client.load_table_from_dataframe(
                        temporary_df_value,
                        temporary_table_id, 
                        job_config=load_table_config
                    )
                    load_table_result = load_table_execute.result()
                    load_table_id = f"{load_table_execute.destination.project}.{load_table_execute.destination.dataset_id}.{load_table_execute.destination.table_id}"
                    load_table_rows = google_bigquery_client.get_table(load_table_id).num_rows
                    print(f"✅ [INGEST] Successfully created temporary Google Ads campaign metadata table {load_table_id} with {load_table_rows} row(s) for batch deletion.")
                    logging.info(f"✅ [INGEST] Successfully created temporary Google Ads campaign metadata table {load_table_id} with {load_table_rows} row(s) for batch deletion.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create temporary Google Ads campaign metadata table {temporary_table_id} for batch deletion due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create temporary Google Ads campaign metadata table {temporary_table_id} for batch deletion due to {e}.")

                try:                        
                    print(f"🔍 [INGEST] Deleting existing rows of Google Ads campaign metadata using batch deletion with unique key(s) {unique_keys_defined}...")
                    logging.info(f"🔍 [INGEST] Deleting existing rows of Google Ads campaign metadata using batch deletion with unique key(s) {unique_keys_defined}...")
                    query_delete_condition = " AND ".join([
                        f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                        for col in unique_keys_config
                    ])
                    query_delete_config = f"""
                        DELETE FROM `{raw_table_campaign}` AS main
                        WHERE EXISTS (
                            SELECT 1 FROM `{temporary_table_id}` AS temp
                            WHERE {query_delete_condition}
                        )
                    """                    
                    query_delete_execute = google_bigquery_client.query(query_delete_config)
                    query_delete_result = query_delete_execute.result()
                    ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                    google_bigquery_client.delete_table(
                        temporary_table_id, 
                        not_found_ok=True
                    )                    
                    print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Google Ads campaign metadata table {raw_table_campaign}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Google Ads campaign metadata table {raw_table_campaign}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to delete existing rows of Google Ads campaign metadata table {raw_table_campaign} by batch deletion due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to delete existing rows of Google Ads campaign metadata table {raw_table_campaign} by batch deletion due to {e}.")
            ingest_sections_status[ingest_section_name] = "succeed"
        
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_campaign} if it not exist for Google Ads campaign metadata due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_campaign} if it not exist for Google Ads campaign metadata due to {e}.")        
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Upload Google Ads campaign metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload Google Ads campaign metadata to Google BigQuery"
        ingest_section_start = time.time()
        
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            load_table_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            load_table_execute = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_campaign, 
                job_config=load_table_config
            )
            load_table_result = load_table_execute.result()
            ingest_rows_uploaded = load_table_execute.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Google Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Google Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload Google Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload Google Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Summarize ingestion results for Google Ads campaign metadata
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = (ingest_df_uploaded.copy() if "ingest_df_uploaded" in locals() and not ingest_df_uploaded.empty else pd.DataFrame())
        ingest_sections_total = len(ingest_sections_status) 
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"] 
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_rows_input = len(ingest_campaign_ids)
        ingest_rows_output = len(ingest_df_final)
        ingest_sections_summary = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys())
        ))
        ingest_sections_detail = {
            ingest_section_summary: {
                "status": ingest_sections_status.get(ingest_section_summary, "unknown"),
                "time": ingest_sections_time.get(ingest_section_summary, None),
            }
            for ingest_section_summary in ingest_sections_summary
        }     
        if ingest_sections_failed:
            ingest_status_final = "ingest_failed_all"
            print(f"❌ [INGEST] Failed to complete Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Google Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")        
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed, 
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded, 
                "ingest_sections_failed": ingest_sections_failed, 
                "ingest_sections_detail": ingest_sections_detail, 
                "ingest_rows_input": ingest_rows_input, 
                "ingest_rows_output": ingest_rows_output
            },
        }
    return ingest_results_final

# 2. INGEST GOOGLE ADS INSIGHTS

# 2.1. Ingest Google Ads campaign insights
def ingest_campaign_insights(
    ingest_date_start: str,
    ingest_date_end: str,
) -> pd.DataFrame:  
    print(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {ingest_date_start} to {ingest_date_end}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {ingest_date_start} to {ingest_date_end}...")

    # 2.1.1. Start timing Google Ads campaign insights ingestion
    ICT = ZoneInfo("Asia/Ho_Chi_Minh") 
    ingest_dates_uploaded = []
    ingest_time_start = time.time()
    ingest_sections_status = {}
    ingest_sections_time = {}
    ingest_loops_time = {
        "[INGEST] Trigger to fetch Google Ads campaign insights": 0.0,
        "[INGEST] Trigger to enforce schema for Google Ads campaign insights": 0.0,
        "[INGEST] Prepare Google BigQuery table_id for ingestion": 0.0,
        "[INGEST] Delete existing rows or create new table if not exist": 0.0,
        "[INGEST] Upload Google Ads campaign insights to Google BigQuery": 0.0,
        "[INGEST] Cooldown before next Google Ads campaign insights fetch": 0.0,
    }
    print(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {ingest_date_start} to {ingest_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.1.2. Initialize Google BigQuery client
        ingest_section_name = "[INGEST] Initialize Google BigQuery client"
        ingest_section_start = time.time()
        
        try:
            print(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [INGEST] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [INGEST] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 2.1.3. Trigger to fetch Google Ads campaign insights
        ingest_date_list = pd.date_range(start=ingest_date_start, end=ingest_date_end).strftime("%Y-%m-%d").tolist()
        for ingest_date_indexed, ingest_date_separated in enumerate(ingest_date_list):    
            
            ingest_section_name = "[INGEST] Trigger to fetch Google Ads campaign insights"
            ingest_section_start = time.time()
            
            try:
                print(f"🔁 [INGEST] Triggering to fetch Google Ads campaigns insights for {ingest_date_separated}...")
                logging.info(f"🔁 [INGEST] Triggering to fetch Google Ads campaigns insights for {ingest_date_separated}...")
                ingest_results_fetched = fetch_campaign_insights(ingest_date_separated, ingest_date_separated)
                ingest_df_fetched = ingest_results_fetched["fetch_df_final"]                
                ingest_summary_fetched = ingest_results_fetched["fetch_summary_final"]
                ingest_status_fetched = ingest_results_fetched["fetch_status_final"]
                if ingest_status_fetched == "fetch_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                elif ingest_status_fetched == "fetch_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [INGEST] Partially triggered Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.warning(f"⚠️ [INGEST] Partially triggered Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) in {ingest_summary_fetched['fetch_time_elapsed']}s.")                    
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Google Ads campaign insights fetching for {ingest_date_separated} with {ingest_summary_fetched['fetch_days_output']}/{ingest_summary_fetched['fetch_days_input']} fetched day(s) due to {', '.join(ingest_summary_fetched['fetch_sections_failed'])} or unknown error in {ingest_summary_fetched['fetch_time_elapsed']}s.")     
            
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)
                
    # 2.1.4. Trigger to enforce schema for Google Ads campaign insights
            ingest_section_name = "[INGEST] Trigger to enforce schema for Google Ads campaign insights"
            ingest_section_start = time.time()
            
            try:
                print(f"🔁 [INGEST] Triggering to enforce schema for Google Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                logging.info(f"🔁 [INGEST] Triggering to enforce schema for Google Ads campaign insights for {ingest_date_separated} with {len(ingest_df_fetched)} row(s)...")
                ingest_results_enforced = enforce_table_schema(schema_df_input=ingest_df_fetched,schema_type_mapping="ingest_campaign_insights")
                ingest_df_enforced = ingest_results_enforced["schema_df_final"]                
                ingest_summary_enforced = ingest_results_enforced["schema_summary_final"]
                ingest_status_enforced = ingest_results_enforced["schema_status_final"]
                if ingest_status_enforced == "schema_succeed_all":
                    ingest_sections_status[ingest_section_name] = "succeed"
                    print(f"✅ [INGEST] Successfully triggered Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.info(f"✅ [INGEST] Successfully triggered Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")                    
                elif ingest_status_enforced == "schema_succeed_partial":
                    ingest_sections_status[ingest_section_name] = "partial"
                    print(f"⚠️ [FETCH] Partially triggered Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.warning(f"⚠️ [FETCH] Partially triggered Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                else:
                    ingest_sections_status[ingest_section_name] = "failed"
                    print(f"❌ [INGEST] Failed to trigger Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
                    logging.error(f"❌ [INGEST] Failed to trigger Google Ads campaign insights schema enforcement for {ingest_date_separated} with {ingest_summary_enforced['schema_rows_output']}/{ingest_summary_enforced['schema_rows_input']} enforced row(s) in {ingest_summary_enforced['schema_time_elapsed']}s.")
            
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)         
        
    # 2.1.5. Prepare Google BigQuery table_id for ingestion
            ingest_section_name = "[INGEST] Prepare Google BigQuery table_id for ingestion"
            ingest_section_start = time.time()
            
            try:
                first_date = pd.to_datetime(ingest_df_fetched["date_start"].dropna().iloc[0])
                y, m = first_date.year, first_date.month
                raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
                raw_table_campaign = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")
                logging.info(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights for {ingest_date_separated} to Google BigQuery table_id {raw_table_campaign}...")
            
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)          

    # 2.1.6. Delete existing rows or create new table if not exist
            ingest_section_name = "[INGEST] Delete existing rows or create new table if not exist"
            ingest_section_start = time.time()
            
            try:
                ingest_df_deduplicated = ingest_df_enforced.drop_duplicates().reset_index(drop=True)
                
                try:
                    print(f"🔍 [INGEST] Checking Google Ads campaign insights table {raw_table_campaign} existence...")
                    logging.info(f"🔍 [INGEST] Checking Google Ads campaign insights table {raw_table_campaign} existence...")
                    google_bigquery_client.get_table(raw_table_campaign)
                    ingest_table_existed = True
                except NotFound:
                    ingest_table_existed = False
                except Exception as e:
                    print(f"❌ [INGEST] Failed to check Google Ads campaign insights table {raw_table_campaign} existence due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to check Google Ads campaign insights table {raw_table_campaign} existence due to {e}.")
                
                if not ingest_table_existed:
                    print(f"⚠️ [INGEST] Google Ads campaign insights table {raw_table_campaign} not found then table creation will be proceeding...")
                    logging.info(f"⚠️ [INGEST] Google Ads campaign insights table {raw_table_campaign} not found then table creation will be proceeding...")
            
            # Define table creation               
                    table_clusters_defined = []
                    table_schemas_defined = []
                    table_partition_defined = "date"   

            # Config table creation
                    table_schemas_config = (
                        table_schemas_defined
                        if table_schemas_defined
                        else [
                            bigquery.SchemaField(
                                col,
                                "INT64" if dtype.name.startswith("int")
                                else "FLOAT64" if dtype.name.startswith("float")
                                else "BOOL" if dtype.name == "bool"
                                else "TIMESTAMP" if "datetime" in dtype.name
                                else "STRING"
                            )
                            for col, dtype in ingest_df_deduplicated.dtypes.items()
                        ]
                    )
             
                    table_partition_config = (
                        table_partition_defined
                        if table_partition_defined in ingest_df_deduplicated.columns
                        else None
                    )
            
                    table_clusters_config = (
                        [col for col in table_clusters_defined if col in ingest_df_deduplicated.columns]
                        if table_clusters_defined
                        else None
                    )
            
            # Execute table creation                
                    try:    
                        print(f"🔍 [INGEST] Creating Google Ads campaign insights table defined name {raw_table_campaign} with partition on {table_partition_config} and cluster on {table_clusters_config}...")
                        logging.info(f"🔍 [INGEST] Creating Google Ads campaign insights table defined name {raw_table_campaign} with partition on {table_partition_config} and cluster on {table_clusters_config}...")
                        create_table_config = bigquery.Table(
                            raw_table_campaign,
                            schema=table_schemas_config
                        )
                        if table_partition_config:
                            create_table_config.time_partitioning = bigquery.TimePartitioning(
                                type_=bigquery.TimePartitioningType.DAY,
                                field=table_partition_config
                            )
                        if table_clusters_config:
                            create_table_config.clustering_fields = table_clusters_config
                        create_table_execute = google_bigquery_client.create_table(create_table_config)
                        create_table_id = create_table_execute.full_table_id
                        print(f"✅ [INGEST] Successfully created Google Ads campaign insights table actual name {create_table_id} with partition on {table_partition_config} and cluster on {table_clusters_config}.")
                        logging.info(f"✅ [INGEST] Successfully created Google Ads campaign insights table actual name {create_table_id} with partition on {table_partition_config} and cluster on {table_clusters_config}.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to create Google Ads campaign insights table {raw_table_campaign} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to create Google Ads campaign insights table {raw_table_campaign} due to {e}.")
                
                else:
                    print(f"🔄 [INGEST] Found Google Ads campaign insights table {raw_table_campaign} then existing rows deletion will be proceeding...")
                    logging.info(f"🔄 [INGEST] Found Google Ads campaign insights table {raw_table_campaign} then existing rows deletion will be proceeding...")
            
            # Define overlapping deletion
                    unique_keys_defined = "date"

            # Config overlapping deletion
                    unique_keys_config = [
                        unique_key_defined
                        for unique_key_defined in unique_keys_defined
                        if unique_key_defined in ingest_df_deduplicated.columns
                    ]                
            
                    if len(unique_keys_config) == 1:
                        unique_keys_value = (
                            ingest_df_deduplicated[unique_keys_config[0]]
                            .dropna()
                            .astype(str)
                            .unique()
                            .tolist()
                        )

                    else:
                        unique_keys_value = (
                            ingest_df_deduplicated[unique_keys_config]
                            .dropna()
                            .astype(str)
                            .drop_duplicates()
                            .apply(tuple, axis=1)
                            .tolist()
                        )

            # Execute overlapping deletion
                    try: 
                        print(f"🔍 [INGEST] Validating overlapping dates in Google Ads campaign insights table {raw_table_campaign}...")
                        logging.info(f"🔍 [INGEST] Validating overlapping dates in Google Ads campaign insights table {raw_table_campaign}...")
                        query_select_config = f"""
                            SELECT DISTINCT {", ".join(unique_keys_config)}
                            FROM `{raw_table_campaign}`
                        """                        
                        query_select_execute = google_bigquery_client.query(query_select_config)
                        query_select_result = query_select_execute.result()
                        ingest_existing_dates = [
                            str(getattr(row, unique_keys_config))
                            for row in query_select_result
                        ]  
                        ingest_new_dates = unique_keys_value                  
                        ingest_dates_overlapped = set(ingest_new_dates) & set(ingest_existing_dates)
                        print(f"✅ [INGEST] Successfully validated {len(ingest_dates_overlapped)} overlapping date(s) in Google Ads campaign insights {raw_table_campaign} table.")
                        logging.info(f"✅ [INGEST] Successfully validated {len(ingest_dates_overlapped)} overlapping date(s) in Google Ads campaign insights {raw_table_campaign} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to validate overlapping dates of Google Ads campaign insights table {raw_table_campaign} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to validate overlapping dates of Google Ads campaign insights table {raw_table_campaign} due to {e}.")                    
                    
                    if ingest_dates_overlapped:
                        print(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Google Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        logging.warning(f"⚠️ [INGEST] Found {len(ingest_dates_overlapped)} overlapping date(s) in Google Ads campaign insights {raw_table_campaign} table then deletion will be proceeding...")
                        
                        for ingest_date_overlapped in ingest_dates_overlapped:                            

            # Execution for table delete query
                            try:
                                print(f"🔍 [INGEST] Deleting existing rows of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}...")
                                logging.info(f"🔍 [INGEST] Deleting existing rows of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}...")
                                query_delete_config = f"""
                                    DELETE FROM `{raw_table_campaign}`
                                    WHERE {unique_keys_config} = @date_value
                                """                                                          
                                query_delete_execute = google_bigquery_client.query(
                                    query_delete_config, 
                                    job_config = bigquery.QueryJobConfig(
                                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", ingest_date_overlapped)]
                                    )
                                )
                                query_delete_result = query_delete_execute.result()
                                ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                                print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                                logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign}.")
                            except Exception as e:
                                print(f"❌ [INGEST] Failed to delete existing rows of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                                logging.error(f"❌ [INGEST] Failed to delete existing rows of Google Ads campaign insights for {ingest_date_overlapped} in Google BigQuery table {raw_table_campaign} due to {e}.")
                    else:
                        print(f"⚠️ [INGEST] No overlapping date of Google Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                        logging.info(f"⚠️ [INGEST] No overlapping date of Google Ads campaign insights found in Google BigQuery {raw_table_campaign} table then deletion is skipped.")
                ingest_sections_status[ingest_section_name] = "succeed"
            
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_campaign} if it not exist for Google Ads campaign insights due to {e}.")
                logging.error(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_campaign} if it not exist for Google Ads campaign insights due to {e}.")
            
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)               

    # 2.1.7. Upload Google Ads campaign insights to Google BigQuery
            ingest_section_name = "[INGEST] Upload Google Ads campaign insights to Google BigQuery"
            ingest_section_start = time.time()

            try:
                print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign}...")
                load_table_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                load_table_execute = google_bigquery_client.load_table_from_dataframe(
                    ingest_df_deduplicated,
                    raw_table_campaign,
                    job_config=load_table_config
                )
                load_table_result = load_table_execute.result()
                ingest_rows_uploaded = load_table_execute.output_rows
                ingest_dates_uploaded.append(ingest_df_deduplicated.copy())
                ingest_sections_status[ingest_section_name] = "succeed"
                print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
                logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign}.")
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")
                logging.error(f"❌ [INGEST] Failed to upload {len(ingest_df_deduplicated)} deduplicated row(s) of Google Ads campaign insights to Google BigQuery table {raw_table_campaign} due to {e}.")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)              

    # 2.1.8. Cooldown before next Google Ads campaign insights fetch
            ingest_section_name = "[INGEST] Cooldown before next Google Ads campaign insights fetch"
            ingest_section_start = time.time()
            try:
                if ingest_date_indexed < len(ingest_date_list) - 1:
                    ingest_cooldown_queued = ingest_results_fetched["fetch_summary_final"].get("fetch_cooldown_queued", 60)
                    print(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Google Ads campaign insights...")
                    logging.info(f"🔁 [INGEST] Waiting {ingest_cooldown_queued}s cooldown before triggering to fetch next day of Google Ads campaign insights...")
                    time.sleep(ingest_cooldown_queued)
                ingest_sections_status[ingest_section_name] = "succeed"
            except Exception as e:
                ingest_sections_status[ingest_section_name] = "failed"
                print(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Google Ads campaign insights due to {e}")
                logging.error(f"❌ [INGEST] Failed to set cooldown for {ingest_cooldown_queued}s before triggering to fetch next day of Google Ads campaign insights due to {e}")
            finally:
                ingest_loops_time[ingest_section_name] += round(time.time() - ingest_section_start, 2)

    # 2.1.9. Summarize ingestion results for Google Ads campaign insights
    finally:
        ingest_time_elapsed = round(time.time() - ingest_time_start, 2)
        ingest_df_final = pd.concat(ingest_dates_uploaded or [], ignore_index=True)
        ingest_sections_total = len(ingest_sections_status)
        ingest_sections_failed = [k for k, v in ingest_sections_status.items() if v == "failed"]
        ingest_sections_succeeded = [k for k, v in ingest_sections_status.items() if v == "succeed"]
        ingest_dates_input = len(ingest_date_list)
        ingest_dates_output = len(ingest_dates_uploaded)
        ingest_dates_failed = ingest_dates_input - ingest_dates_output
        ingest_rows_output = ingest_rows_uploaded
        ingest_section_all = list(dict.fromkeys(
            list(ingest_sections_status.keys()) +
            list(ingest_sections_time.keys()) +
            list(ingest_loops_time.keys())
        ))
        ingest_sections_detail = {}
        for ingest_section_separated in ingest_section_all:
            ingest_section_time = (
                ingest_loops_time.get(ingest_section_separated)
                if ingest_section_separated in ingest_loops_time
                else ingest_sections_time.get(ingest_section_separated)
            )
            ingest_sections_detail[ingest_section_separated] = {
                "status": ingest_sections_status.get(ingest_section_separated, "unknown"),
                "time": round(ingest_section_time or 0.0, 2),
                "type": "loop" if ingest_section_separated in ingest_loops_time else "single"
            }
        if ingest_sections_failed:
            print(f"❌ [INGEST] Failed to complete Google Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete Google Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_failed_all"
        elif ingest_dates_output == ingest_dates_input:
            print(f"🏆 [INGEST] Successfully completed Google Ads campaign insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed Google Ads campaign insights ingestion from from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_all"            
        else:
            print(f"⚠️ [INGEST] Partially completed Google Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed Google Ads campaign insights ingestion from {ingest_date_start} to {ingest_date_end} with {ingest_dates_output}/{ingest_dates_input} ingested day(s) and {ingest_rows_output} ingested row(s) in {ingest_time_elapsed}s.")
            ingest_status_final = "ingest_succeed_partial"
        ingest_results_final = {
            "ingest_df_final": ingest_df_final,
            "ingest_status_final": ingest_status_final,
            "ingest_summary_final": {
                "ingest_time_elapsed": ingest_time_elapsed,
                "ingest_sections_total": ingest_sections_total,
                "ingest_sections_succeed": ingest_sections_succeeded,
                "ingest_sections_failed": ingest_sections_failed,
                "ingest_sections_detail": ingest_sections_detail,
                "ingest_dates_input": ingest_dates_input,
                "ingest_dates_output": ingest_dates_output,
                "ingest_dates_failed": ingest_dates_failed,
                "ingest_rows_output": ingest_rows_output,
            }
        }
    return ingest_results_final