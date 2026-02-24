{{ config(materialized='ephemeral') }}

{% set company = var('company') %}
{% set department = var('department') %}
{% set account = var('account') %}

{% set raw_schema = company ~ '_dataset_google_api_raw' %}
{% set table_prefix = company ~ '_table_google_' ~ department ~ '_' ~ account ~ '_campaign_' %}

{% if execute %}

    {% set tables_query %}
        select table_name
        from `{{ target.project }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}m______'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set table_names = results.columns[0].values() if results is not none else [] %}

{% else %}

    {% set table_names = [] %}

{% endif %}

{% if table_names | length == 0 %}

    select
        cast(null as string)  as customer_id,
        cast(null as string)  as campaign_id,

        cast(null as int64)   as impressions,
        cast(null as int64)   as clicks,
        cast(null as numeric) as spend,
        cast(null as int64)   as conversions,
        cast(null as numeric) as conversion_value,

        cast(null as date)    as date,    
        cast(null as int64)   as year,
        cast(null as string)  as month

    where false

{% else %}

{% for table_name in table_names %}

select
    customer_id,
    campaign_id,
    DATE(date) as date,
    month,
    year,
    impressions,
    clicks,
    spend,
    conversions,
    conversion_value
from `{{ target.project }}.{{ raw_schema }}.{{ table_name }}`
{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}