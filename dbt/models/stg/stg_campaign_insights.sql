{{ config(materialized='ephemeral') }}

{% set raw_schema = var('company') ~ '_dataset_google_api_raw' %}
{% set table_prefix = var('company') ~ '_table_google_' ~ var('department') ~ '_' ~ var('account') ~ '_campaign_' %}

{% if execute %}

    {% set tables_query %}
        select table_name
        from `{{ var('project') }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}m______'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set table_names = results.columns[0].values() %}

{% else %}

    {% set table_names = [] %}

{% endif %}

{% if table_names | length == 0 %}

    select
        null as customer_id,
        null as campaign_id,
        null as date,
        null as impressions,
        null as clicks,
        null as cost,
        null as conversions,
        null as conversion_value
    where false

{% else %}

{% for table_name in table_names %}

select
    customer_id,
    campaign_id,
    date,
    impressions,
    clicks,
    cost,
    conversions,
    conversion_value
from `{{ var('project') }}.{{ raw_schema }}.{{ table_name }}`
{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}