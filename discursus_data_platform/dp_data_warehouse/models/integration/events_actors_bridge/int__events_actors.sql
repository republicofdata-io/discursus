with s_named_entities as (

    select * from {{ ref('int__actors') }}

),

s_events_observations as (

    select * from {{ ref('int__events_observations') }}

),

merge_sources as (

    select distinct
        s_events_observations.event_date,
        s_events_observations.action_geo_latitude,
        s_events_observations.action_geo_longitude,
        s_events_observations.action_geo_full_name,
        s_named_entities.actor_name,
        s_named_entities.actor_type
    
    from s_events_observations
    inner join s_named_entities on s_events_observations.observation_url = s_named_entities.article_url

)


select * from merge_sources
