{{ 
  config(
    materialized = 'ephemeral'
  ) 
}}

select
    customer_id,
    campaign_id,
    date,

    impressions,
    clicks,
    cost,
    conversions,
    conversion_value

from `{{ env_var('PROJECT') }}.{{ env_var('COMPANY') }}_dataset_google_api_raw.{{ env_var('COMPANY') }}_table_google_{{ env_var('DEPARTMENT') }}_{{ env_var('ACCOUNT') }}_campaign_*`

where _table_suffix like 'm%'