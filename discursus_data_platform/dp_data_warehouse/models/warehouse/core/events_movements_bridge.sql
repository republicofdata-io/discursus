{{ 
    config(
        unique_key='event_movement_pk'
    )
}}

with s_event_movements as (

    select * from {{ ref('int__events_movements') }}

),

bridge as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_fk,
        {{ dbt_utils.generate_surrogate_key([
            'movement_name'
        ]) }} as movement_fk

    from s_event_movements

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'event_fk',
            'movement_fk'
        ]) }} as event_movement_pk,
        *
    
    from bridge

)

select * from final
