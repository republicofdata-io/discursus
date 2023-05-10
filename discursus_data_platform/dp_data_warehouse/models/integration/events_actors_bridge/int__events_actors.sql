with s_named_entities as (

    select * from {{ ref('stg__gdelt__mention_named_entities') }}

),

s_events_observations as (

    select * from {{ ref('int__events_observations') }}

),

merge_sources as (

    select distinct
        s_events_observations.published_date,
        s_events_observations.movement_name,
        s_events_observations.action_geo_latitude,
        s_events_observations.action_geo_longitude,
        s_named_entities.entity_name as actor_name
    
    from s_events_observations
    inner join s_named_entities on s_events_observations.observation_url = s_named_entities.mention_url

)


select * from merge_sources
