{{ 
    config(
        unique_key='event_pk',
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_events as (

    select * from {{ ref('int__events') }}

),

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'movement_name',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_pk, 
        {{ dbt_utils.generate_surrogate_key(['movement_name']) }} as movement_fk,

        event_date, 
        action_geo_country_code,
        action_geo_country_name,
        action_geo_full_name,
        action_geo_latitude, 
        action_geo_longitude

    from s_events

)

select * from final
