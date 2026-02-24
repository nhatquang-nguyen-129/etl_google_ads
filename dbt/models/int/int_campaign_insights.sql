{{ config(materialized='ephemeral') }}

select
    date,
    month,
    year,

    i.customer_id,
    i.campaign_id,

    m.campaign_name,

    case
        when m.campaign_status = 'ENABLED' then '🟢'
        when m.campaign_status = 'PAUSED'  then '⚪'
        when m.campaign_status = 'REMOVED' then '🔴'
        else '❓'
    end as campaign_status,

    m.platform,
    m.objective,
    m.budget_group_1,
    m.budget_group_2,
    m.region,
    m.category_level_1,
    m.track_group,
    m.pillar_group,
    m.content_group,

    i.impressions,
    i.clicks,
    i.spend,
    i.conversions,
    i.conversion_value

from {{ ref('stg_campaign_insights') }} i
left join `{{ target.project }}.{{ var('company') }}_dataset_google_api_raw.{{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` m
    on i.customer_id = m.customer_id
   and i.campaign_id = m.campaign_id