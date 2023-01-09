{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_events as (

    select distinct
        movement_name,
        published_date as event_date, 
        action_geo_country_code,
        action_geo_country_name,
        action_geo_full_name,
        action_geo_latitude, 
        action_geo_longitude
    
    from {{ ref('int__events__3_associate_movements') }}

)

select * from s_events