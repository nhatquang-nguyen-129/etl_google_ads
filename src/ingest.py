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

# Add internal Facebook modules for handling
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

    # 1.1.6. Delete existing rows or create new table if it not exist
        ingest_section_name = "[INGEST] Delete existing row(s) or create new table if it not exist"
        ingest_section_start = time.time()
        try:
            ingest_df_deduplicated = ingest_df_enforced.drop_duplicates()
            try:
                print(f"🔍 [INGEST] Checking Google Ads campaign metadata table {raw_table_campaign} existence...")
                logging.info(f"🔍 [INGEST] Checking Google Ads campaign metadata table {raw_table_campaign} existence...")
                google_bigquery_client.get_table(raw_table_campaign)
                ingest_table_existed = True
            except NotFound:
                ingest_table_existed = False
            except Exception:
                print(f"❌ [INGEST] Failed to check Google Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
                logging.error(f"❌ [INGEST] Failed to check Google Ads campaign metadata table {raw_table_campaign} existence due to {e}.")
            if not ingest_table_existed:
                print(f"⚠️ [INGEST] Budget Allocation table {raw_table_budget} not found then table creation will be proceeding...")
                logging.info(f"⚠️ [INGEST] Budget Allocation table {raw_table_budget} not found then table creation will be proceeding...")
        
        # Configuration for table creation               
                table_schemas_defined = []
                table_clusters_defined = ["raw_date_month"]
                table_partition_defined = "date"        

        # Definition for table schemas
                if not table_schemas_defined:
                    table_schemas_effective = []
                    for col, dtype in ingest_df_deduplicated.dtypes.items():
                        if dtype.name.startswith("int"):
                            bq_type = "INT64"
                        elif dtype.name.startswith("float"):
                            bq_type = "FLOAT64"
                        elif dtype.name == "bool":
                            bq_type = "BOOL"
                        elif "datetime" in dtype.name:
                            bq_type = "TIMESTAMP"
                        else:
                            bq_type = "STRING"
                        table_schemas_effective.append(bigquery.SchemaField(col, bq_type))
                else:
                    table_schemas_effective = table_schemas_defined                                    
        
        # Definition for table partition     
                table_partition_effective = (
                    table_partition_defined
                    if table_partition_defined in ingest_df_deduplicated.columns
                    else None
                )
        
        # Definition for table clusters
                table_clusters_effective = (
                    [c for c in table_clusters_defined if c in ingest_df_deduplicated.columns]
                    if table_clusters_defined
                    else None
                )
        
        # Execute table creation                
                try:    
                    print(f"🔍 [INGEST] Creating Budget Allocation table defined name {raw_table_budget} with partition on {table_partition_effective} and cluster on {table_clusters_effective}...")
                    logging.info(f"🔍 [INGEST] Creating Budget Allocation table defined name {raw_table_budget} with partition on {table_partition_effective} and cluster on {table_clusters_effective}...")
                    table_configuration_defined = bigquery.Table(
                        raw_table_budget,
                        schema=table_schemas_effective
                    )
                    if table_partition_effective:
                        table_configuration_defined.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=table_partition_effective
                        )
                    if table_clusters_effective:
                        table_configuration_defined.clustering_fields = table_clusters_effective
                    query_table_create = google_bigquery_client.create_table(table_configuration_defined)
                    query_table_id = query_table_create.full_table_id
                    print(f"✅ [INGEST] Successfully created Budget Allocation table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_effective}.")
                    logging.info(f"✅ [INGEST] Successfully created Budget Allocation table actual name {query_table_id} with partition on {table_partition_effective} and cluster on {table_clusters_effective}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create Budget Allocation table {raw_table_budget} due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create Budget Allocation table {raw_table_budget} due to {e}.")
            else:
                print(f"🔄 [INGEST] Found raw Budget Allocation table {raw_table_budget} then existing row(s) deletion will be proceeding...")
                logging.info(f"🔄 [INGEST] Found raw Budget Allocation table {raw_table_budget} then existing row(s) deletion will be proceeding...")            
        
        # Configuration for table delete keys
                unique_keys_defined = ["raw_date_month"]                
        
        # Definition for table delete keys
                temporary_table_id = f"{PROJECT}.{raw_dataset}.temp_table_campaign_metadata_delete_keys_{uuid.uuid4().hex[:8]}"
                unique_keys_effective = (
                        ingest_df_deduplicated[unique_keys_defined]
                        .dropna()
                        .drop_duplicates()
                        if unique_keys_defined
                        else None
                )

        # Execute temporary table creation         
                try:
                    print(f"🔍 [INGEST] Creating temporary table contains duplicated Budget Allocation unique keys for batch deletion...")
                    logging.info(f"🔍 [INGEST] Creating temporary table contains duplicated Budget Allocation unique keys for batch deletion...")
                    job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    job_load_load = google_bigquery_client.load_table_from_dataframe(
                        unique_keys_effective, 
                        temporary_table_id, 
                        job_config=job_load_config
                    )
                    job_load_result = job_load_load.result()
                    created_table_id = f"{job_load_load.destination.project}.{job_load_load.destination.dataset_id}.{job_load_load.destination.table_id}"
                    print(f"✅ [INGEST] Successfully created temporary Budget Allocation table {created_table_id} for batch deletion.")
                    logging.info(f"✅ [INGEST] Successfully created temporary Budget Allocation table {created_table_id} for batch deletion.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to create temporary Budget Allocation table {temporary_table_id} for batch deletion due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to create temporary Budget Allocation table {temporary_table_id} for batch deletion due to {e}.")

        # Configuration for table delete query
                query_delete_condition = " AND ".join([
                    f"CAST(main.{col} AS STRING) = CAST(temp.{col} AS STRING)"
                    for col in unique_keys_effective
                ])
                query_delete_config = f"""
                    DELETE FROM `{raw_table_budget}` AS main
                    WHERE EXISTS (
                        SELECT 1 FROM `{temporary_table_id}` AS temp
                        WHERE {query_delete_condition}
                    )
                """

        # Execute batch delete                
                try:                        
                    print(f"🔍 [INGEST] Deleting existing row of Budget Allocation using batch deletion with unique key(s) {unique_keys_defined}...")
                    logging.info(f"🔍 [INGEST] Deleting existing row of Budget Allocation using batch deletion with unique key(s) {unique_keys_defined}...")
                    query_delete_load = google_bigquery_client.query(query_delete_config)
                    query_delete_result = query_delete_load.result()
                    ingest_rows_deleted = query_delete_result.num_dml_affected_rows
                    google_bigquery_client.delete_table(
                        temporary_table_id, 
                        not_found_ok=True
                    )                    
                    print(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Budget Allocation table {raw_table_budget}.")
                    logging.info(f"✅ [INGEST] Successfully deleted {ingest_rows_deleted} existing row(s) of Budget Allocation table {raw_table_budget}.")
                except Exception as e:
                    print(f"❌ [INGEST] Failed to delete existing rows of Budget Allocation table {raw_table_budget} by batch deletion due to {e}.")
                    logging.error(f"❌ [INGEST] Failed to delete existing rows of Budget Allocation table {raw_table_budget} by batch deletion due to {e}.")
            ingest_sections_status[ingest_section_name] = "succeed"
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_budget} if it not exist for Budget Allocation due to {e}.")
            logging.error(f"❌ [INGEST] Failed to delete existing rows or create new table {raw_table_budget} if it not exist for Budget Allocation due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.7. Upload TikTok Ads campaign metadata to Google BigQuery
        ingest_section_name = "[INGEST] Upload TikTok Ads campaign metadata to Google BigQuery"
        ingest_section_start = time.time()
        try:
            print(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            logging.info(f"🔍 [INGEST] Uploading {len(ingest_df_deduplicated)} deduplicated row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}...")
            job_load_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job_load_load = google_bigquery_client.load_table_from_dataframe(
                ingest_df_deduplicated, 
                raw_table_campaign, 
                job_config=job_load_config
            )
            job_load_result = job_load_load.result()
            ingest_rows_uploaded = job_load_load.output_rows
            ingest_df_uploaded = ingest_df_deduplicated.copy()
            ingest_sections_status[ingest_section_name] = "succeed"
            print(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
            logging.info(f"✅ [INGEST] Successfully uploaded {ingest_rows_uploaded} row(s) of TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign}.")
        except Exception as e:
            ingest_sections_status[ingest_section_name] = "failed"
            print(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
            logging.error(f"❌ [INGEST] Failed to upload TikTok Ads campaign metadata to Google BigQuery table {raw_table_campaign} due to {e}.")
        finally:
            ingest_sections_time[ingest_section_name] = round(time.time() - ingest_section_start, 2)

    # 1.1.8. Summarize ingestion results for TikTok Ads campaign metadata
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
            print(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")
            logging.error(f"❌ [INGEST] Failed to complete TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) due to {', '.join(ingest_sections_failed)} failed section(s) in {ingest_time_elapsed}s.")            
        elif ingest_rows_output == ingest_rows_input:
            ingest_status_final = "ingest_succeed_all"
            print(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.info(f"🏆 [INGEST] Successfully completed TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")    
        else:
            ingest_status_final = "ingest_succeed_partial"
            print(f"⚠️ [INGEST] Partially completed TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")
            logging.warning(f"⚠️ [INGEST] Partially completed TikTok Ads campaign metadata ingestion with {ingest_rows_output}/{ingest_rows_input} ingested row(s) in {ingest_time_elapsed}s.")        
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


# 2. INGEST GOOGLE ADS INSIGHTS FROM GOOGLE ADS API TO GOOGLE BIGQUERY

# 2.1. Ingest campaign insights for Google Ads
def ingest_google_campaign_insights(
    start_date: str,
    end_date: str,
    write_disposition: str = "WRITE_APPEND"
) -> pd.DataFrame:
    
    print(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {start_date} to {end_date}...")
    logging.info(f"🚀 [INGEST] Starting to ingest Google Ads campaign insights from {start_date} to {end_date}...")

    # 2.1.1. Call Google Ads API to fetch campaign insights
    print("🔍 [INGEST] Triggering to fetch Google Ads campaigns insights from API...")
    logging.info("🔍 [INGEST] Triggering to fetch Google Ads campaigns insights from API...")
    df = fetch_campaign_insights(start_date, end_date)    
    if df.empty:
        print("⚠️ [INGEST] Empty Google Ads campaign insights returned.")
        logging.warning("⚠️ [INGEST] Empty Google Ads campaign insights returned.")    
        return df

    # 2.1.2. Prepare table_id
    first_date = pd.to_datetime(df["date"].dropna().iloc[0])
    y, m = first_date.year, first_date.month
    raw_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_raw"
    table_id = f"{PROJECT}.{raw_dataset}.{COMPANY}_table_{PLATFORM}_{DEPARTMENT}_{ACCOUNT}_campaign_m{m:02d}{y}"
    print(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")
    logging.info(f"🔍 [INGEST] Proceeding to ingest Google Ads campaign insights from {start_date} to {end_date} with table_id {table_id}...")

    # 2.1.3. Enrich Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Trigger to enrich Google Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        logging.info(f"🔁 [INGEST] Trigger to enrich Google Ads campaign insights from {start_date} to {end_date} with {len(df)} row(s)...")
        df = enrich_campaign_insights(df)
        df["account_name"] = fetch_account_name()
        df["date_range"] = f"{start_date}_to_{end_date}"
        df["last_updated_at"] = datetime.utcnow().replace(tzinfo=pytz.UTC)
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger enrichment Google Ads campaign insights from {start_date} to {end_date} due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger enrichment Google Ads campaign insights from {start_date} to {end_date} due to {e}.")
        raise

    # 2.1.4. Cast Google Ads numeric fields to float
    try:
        numeric_fields = [
            "impressions",
            "clicks",
            "spend",        
            "conversions",
            "all_conversions",
        ]
        print(f"🔁 [INGEST] Casting Google Ads campaign insights {numeric_fields} numeric field(s)...")
        logging.info(f"🔁 [INGEST] Casting Google Ads campaign insights {numeric_fields} numeric field(s)...")
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"✅ [INGEST] Successfully casted Google Ads campaign insights {numeric_fields} numeric field(s) to float.")
        logging.info(f"✅ [INGEST] Successfully casted Google Ads campaign insights {numeric_fields} numeric field(s) to float.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to cast Google Ads numeric field(s) to float due to {e}.")
        logging.error(f"❌ [INGEST] Failed to cast Google Ads numeric field(s) to float due to {e}.")
        raise

    # 2.1.5. Enforce schema for Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign insights...")
        logging.info(f"🔁 [INGEST] Triggering to enforce schema for {len(df)} row(s) of Google Ads campaign insights...")
        df = ensure_table_schema(df, schema_type="ingest_google_campaign_insights")
    except Exception as e:
        print(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to trigger schema enforcement for Google Ads campaign insights due to {e}.")
        raise

    # 2.1.6. Parse date column(s) for Google Ads campaign insights
    try:
        print(f"🔁 [INGEST] Parsing Google Ads campaign insights {df.columns.tolist()} date column(s)...")
        logging.info(f"🔁 [INGEST] Parsing Google Ads campaign insights {df.columns.tolist()} date column(s)...")
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["date_start"] = df["date"].dt.strftime("%Y-%m-%d")
        print(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Google Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully parsed {df.columns.tolist()} date column(s) for Google Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to parse date column(s) for Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to parse date column(s) for Google Ads campaign insights due to {e}.")
        raise

    # 2.1.7. Delete existing row(s) or create new table if not exist
    try:
        print(f"🔍 [INGEST] Checking Google Ads campaign insights table {table_id} existence...")
        logging.info(f"🔍 [INGEST] Checking Google Ads campaign insights table {table_id} existence...")
        df = df.drop_duplicates()
        try:
            client = bigquery.Client(project=PROJECT)
        except DefaultCredentialsError as e:
            raise RuntimeError("❌ [INGEST] Failed to initialize Google BigQuery client due to credentials error.") from e
        try:
            client.get_table(table_id)
            table_exists = True
        except Exception:
            table_exists = False
        if not table_exists:
            print(f"⚠️ [INGEST] Google Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            logging.warning(f"⚠️ [INGEST] Google Ads campaign insights table {table_id} not found then table creation will be proceeding...")
            schema = []
            for col, dtype in df.dtypes.items():
                if dtype.name.startswith("int"):
                    bq_type = "INT64"
                elif dtype.name.startswith("float"):
                    bq_type = "FLOAT64"
                elif dtype.name == "bool":
                    bq_type = "BOOL"
                elif "datetime" in dtype.name:
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                schema.append(bigquery.SchemaField(col, bq_type))
            table = bigquery.Table(table_id, schema=schema)
            effective_partition = "date" if "date" in df.columns else None
            if effective_partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=effective_partition
                )
                print(f"🔍 [INGEST] Creating Google Ads campaign insights {table_id} using partition on {effective_partition}...")
                logging.info(f"🔍 [INGEST] Creating Google Ads campaign insights {table_id} using partition on {effective_partition}...")
            table = client.create_table(table)
            print(f"✅ [INGEST] Successfully created Google Ads campaign insights table {table_id} with partition on {effective_partition}.")
            logging.info(f"✅ [INGEST] Successfully created Google Ads campaign insights table {table_id} with partition on {effective_partition}.")
        else:
            new_dates = df["date_start"].dropna().unique().tolist()
            query_existing = f"SELECT DISTINCT date_start FROM `{table_id}`"
            existing_dates = [row.date_start for row in client.query(query_existing).result()]
            overlap = set(new_dates) & set(existing_dates)
            if overlap:
                print(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Google Ads campaign insights {table_id} table then deletion will be proceeding...")
                logging.warning(f"⚠️ [INGEST] Found {len(overlap)} overlapping date(s) {overlap} in Google Ads campaign insights {table_id} table then deletion will be proceeding...")
                for date_val in overlap:
                    query = f"""
                        DELETE FROM `{table_id}`
                        WHERE date_start = @date_value
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("date_value", "STRING", date_val)]
                    )
                    try:
                        result = client.query(query, job_config=job_config).result()
                        deleted_rows = result.num_dml_affected_rows
                        print(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Google Ads campaign insights {table_id} table.")
                        logging.info(f"✅ [INGEST] Successfully deleted {deleted_rows} existing row(s) for {date_val} in Google Ads campaign insights {table_id} table.")
                    except Exception as e:
                        print(f"❌ [INGEST] Failed to delete existing rows in Google Ads campaign insights {table_id} table for {date_val} due to {e}.")
                        logging.error(f"❌ [INGEST] Failed to delete existing rows in Google Ads campaign insights {table_id} table for {date_val} due to {e}.")
            else:
                print(f"✅ [INGEST] No overlapping dates found in Google Ads campaign insights {table_id} table then deletion is skipped.")
                logging.info(f"✅ [INGEST] No overlapping dates found in Google Ads campaign insights {table_id} table then deletion is skipped.")
    except Exception as e:
        print(f"❌ [INGEST] Failed during Google Ads campaign insights ingestion due to {e}.")
        logging.error(f"❌ [INGEST] Failed during Google Ads campaign insights ingestion due to {e}.")
        raise

    # 2.1.8. Upload to BigQuery
    try:
        print(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign insights to table {table_id}...")
        logging.info(f"🔍 [INGEST] Uploading {len(df)} row(s) of Google Ads campaign insights to table {table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()
        print(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign insights.")
        logging.info(f"✅ [INGEST] Successfully uploaded {len(df)} row(s) of Google Ads campaign insights.")
    except Exception as e:
        print(f"❌ [INGEST] Failed to upload Google Ads campaign insights due to {e}.")
        logging.error(f"❌ [INGEST] Failed to upload Google Ads campaign insights due to {e}.")
        raise

    return df