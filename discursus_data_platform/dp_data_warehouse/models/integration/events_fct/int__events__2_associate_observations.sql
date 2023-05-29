with s_events as (

    select * from {{ ref('int__events__1_base') }}

),

s_observations as (

    select * from {{ ref('int__observations__1_base') }}

),

final as (

    select distinct
        s_events.action_geo_full_name,
        s_events.action_geo_country_code,
        s_events.action_geo_country_name,
        s_events.action_geo_state_name,
        s_events.action_geo_city_name,
        s_events.action_geo_latitude, 
        s_events.action_geo_longitude,
        s_events.action_geo_h3_r3,
        s_observations.published_date,
        s_observations.observation_type,
        s_observations.observation_url,
        s_observations.observation_page_title,
        s_observations.observation_summary,
        s_observations.observation_keywords,
        s_observations.observation_source

    from s_events
    inner join s_observations on cast(s_events.gdelt_event_natural_key as string) = s_observations.gdelt_event_sk

)

select * from final