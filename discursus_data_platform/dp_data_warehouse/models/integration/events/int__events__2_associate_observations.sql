with s_events as (

    select * from {{ ref('int__events__1_base') }}

),

s_observations as (

    select * from {{ ref('int__observations__1_base') }}

),

final as (

    select distinct
        s_events.action_geo_country_code,
        s_events.action_geo_country_name,
        s_events.action_geo_full_name, 
        s_events.action_geo_latitude, 
        s_events.action_geo_longitude, 
        s_observations.published_date,
        s_observations.observation_type,
        s_observations.observation_url,
        s_observations.observation_page_title,
        s_observations.observation_page_description,
        s_observations.observation_keywords

    from s_events
    inner join s_observations using (gdelt_event_natural_key)

)

select * from final