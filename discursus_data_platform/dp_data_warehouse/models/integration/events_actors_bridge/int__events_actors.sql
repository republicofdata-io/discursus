{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "event_date||'-'||article_source_date||'-'||movement_name",
    )
}}

with s_named_entities as (

    select * from {{ ref('int__actors') }}

    {% if is_incremental() %}
        where article_source_date >= (select max(article_source_date) from {{ this }})
    {% else %}
        where article_source_date >= dateadd(week, -52, current_date)
    {% endif %}

),

s_events_observations as (

    select * from {{ ref('int__events_observations') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, current_date)
    {% endif %}

),

merge_sources as (

    select distinct
        s_events_observations.event_date,
        s_named_entities.article_source_date,
        s_events_observations.movement_name,
        s_events_observations.action_geo_full_name,
        s_events_observations.action_geo_country_name,
        s_events_observations.action_geo_state_name,
        s_events_observations.action_geo_city_name,
        s_events_observations.action_geo_latitude,
        s_events_observations.action_geo_longitude,
        s_named_entities.actor_name,
        s_named_entities.actor_type
    
    from s_events_observations
    inner join s_named_entities on s_events_observations.observation_url = s_named_entities.article_url

)


select * from merge_sources
