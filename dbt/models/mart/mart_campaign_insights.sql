{{ 
  config(
    materialized = 'table',

    schema = var('company') ~ '_dataset_google_api_mart',
    alias  = var('company') ~ '_table_google_all_all_campaign_performance',

    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "campaign_id"]
  ) 
}}

select
    date,
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,
    impressions,
    clicks,
    cost,
    conversions,
    conversion_value
from {{ ref('int_campaign_insights') }}