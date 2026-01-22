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

from `{{ var('project') }}.{{ var('company') }}_dataset_google_api_raw.
      {{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_*`

where _table_suffix like 'm%'