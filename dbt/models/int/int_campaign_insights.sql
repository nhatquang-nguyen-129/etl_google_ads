{{ config(materialized='ephemeral') }}

select
    date(i.date) as date,

    i.customer_id,
    i.campaign_id,

    m.campaign_name,
    m.campaign_status,
    m.platform,
    m.objective,
    m.budget_group,
    m.region,
    m.category_level_1,
    m.track_group,
    m.pillar_group,
    m.content_group,

    i.impressions,
    i.clicks,
    i.cost,
    i.conversions,
    i.conversion_value

from {{ ref('stg_campaign_insights') }} i
left join `{{ target.project }}.{{ var('company') }}_dataset_google_api_raw.{{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` m
    on i.customer_id = m.customer_id
   and i.campaign_id = m.campaign_id