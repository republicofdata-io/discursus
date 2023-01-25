{{ 
    config(
        unique_key='event_observation_pk',
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_observations as (

    select * from {{ ref('int__observations') }}

),

bridge as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'observation_url'
        ]) }} as observation_fk, 
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'movement_name',
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
