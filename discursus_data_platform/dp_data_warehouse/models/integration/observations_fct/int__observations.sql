with s_observations as (

    select * from {{ ref('int__events__3_associate_movements') }}

), 

final as (

    select
        published_date,
        movement_name,
        action_geo_latitude,
        action_geo_longitude,
        split(split(observation_url, '//')[1], '/')[0]::string as observer_domain,
        observation_type,
        observation_url,
        observation_page_title,
        observation_summary,
        observation_keywords
    
    from s_observations

)

select * from final