{{ 
  config(
    materialized = 'ephemeral'
  ) 
}}

select
    i.date,
    i.customer_id,
    i.campaign_id,

    m.campaign_name,
    m.campaign_status,
    m.serving_status,
    m.advertising_channel_type,
    m.bidding_strategy_type,

    i.impressions,
    i.clicks,
    i.cost,
    i.conversions,
    i.conversion_value

from {{ ref('stg_campaign_insights') }} i
left join `{{ var('project') }}.{{ var('company') }}_dataset_google_api_raw.
          {{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` m
    on i.customer_id = m.customer_id
   and i.campaign_id = m.campaign_id