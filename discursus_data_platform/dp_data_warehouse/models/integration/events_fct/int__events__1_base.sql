with s_historical_gdelt_events as (

    select
        gdelt_event_sk,
        published_date as event_date,
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        action_geo_state_name,
        action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude,
        action_geo_h3_r3,
        'gdelt historical' as event_source

    from {{ ref('stg__gdelt__events') }}

),

s_gdelt_articles as (

    select
        gdelt_event_sk,
        creation_date as event_date,
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

),

merge_sources as (

    select * from s_historical_gdelt_events
    union all
    select * from s_gdelt_articles

)

select * from merge_sources