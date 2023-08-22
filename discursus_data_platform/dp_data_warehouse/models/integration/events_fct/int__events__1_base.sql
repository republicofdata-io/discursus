{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'gdelt_event_sk'
    )
}}

with s_gdelt_articles as (

    select
        gdelt_event_sk,
        creation_date as event_date,
        source_file_date as article_source_file_date,
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        action_geo_state_name,
        action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude,
        action_geo_h3_r3,
        'gdelt' as event_source
        
    from {{ ref('stg__gdelt__articles') }}

    {% if is_incremental() %}
        where source_file_date >= (select max(article_source_file_date) from {{ this }})
    {% else %}
        where source_file_date >= dateadd(week, -52, source_file_date)
    {% endif %}

)

select * from s_gdelt_articles