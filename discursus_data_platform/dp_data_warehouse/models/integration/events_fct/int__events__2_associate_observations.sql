{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "event_date||'-'||action_geo_full_name||'-'||published_date",
    )
}}

with s_events as (

    select * from {{ ref('int__events__1_base') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

),

s_observations as (

    select * from {{ ref('int__observations__1_base') }}

    {% if is_incremental() %}
        where published_date >= (select max(published_date) from {{ this }})
    {% else %}
        where published_date >= dateadd(week, -52, published_date)
    {% endif %}

),

final as (

    select distinct
        s_events.event_date,
        s_events.action_geo_full_name,
        s_events.action_geo_country_code,
        s_events.action_geo_country_name,
        s_events.action_geo_state_name,
        s_events.action_geo_city_name,
        s_events.action_geo_latitude, 
        s_events.action_geo_longitude,
        s_events.action_geo_h3_r3,
        s_events.event_source,
        s_observations.published_date,
        s_observations.observation_type,
        s_observations.observation_url,
        s_observations.observation_page_title,
        s_observations.observation_summary,
        s_observations.observation_keywords,
        s_observations.observation_source

    from s_events
    inner join s_observations using (gdelt_event_sk)

)

select * from final