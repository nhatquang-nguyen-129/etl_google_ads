{{ 
  config(
    materialized = 'table',
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