{{ 
  config(
    alias = var('company') ~ '_table_google_all_all_campaign_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "campaign_id"]
  ) 
}}

select
    date,
    month,
    year,
    
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,
    impressions,
    clicks,
    spend,
    conversions,
    conversion_value,
    platform,
    objective,
    budget_group_1,
    budget_group_2,
    region,
    category_level_1,
    track_group,
    pillar_group,
    content_group
from {{ ref('int_campaign_insights') }}