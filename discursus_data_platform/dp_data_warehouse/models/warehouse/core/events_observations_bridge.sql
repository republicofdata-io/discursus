{{ 
    config(
        unique_key='event_observation_pk'
    )
}}

with s_observations as (

    select * from {{ ref('int__events_observations') }}

),

bridge as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'observation_url'
        ]) }} as observation_fk, 
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_fk

    from s_observations

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'observation_fk',
            'event_fk'
        ]) }} as event_observation_pk,
        *
    
    from bridge

)

select * from final
