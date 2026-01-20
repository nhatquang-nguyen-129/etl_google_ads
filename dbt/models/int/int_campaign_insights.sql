{{ config(materialized='ephemeral') }}

select
    i.date,
    i.account_id,
    i.campaign_id,
    i.ad_id,

    m.campaign_name,
    m.account_name,
    m.delivery_status,

    c.creative_name,
    c.creative_type,

    i.impressions,
    i.clicks,
    i.spend
from {{ source('facebook_ads_raw', 'ad_insights') }} i
left join {{ source('facebook_ads_raw', 'ad_metadata') }} m
  on i.campaign_id = m.campaign_id
 and i.account_id  = m.account_id
left join {{ source('facebook_ads_raw', 'ad_creatives') }} c
  on i.ad_id = c.ad_id