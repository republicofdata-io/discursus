{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "event_date||'-'||movement_name",
    )
}}

with s_events as (

    select distinct
        event_date,
        movement_name,
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        action_geo_state_name,
        action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude,
        action_geo_h3_r3,
        event_source
    
    from {{ ref('int__events__3_associate_movements') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

)

select * from s_events