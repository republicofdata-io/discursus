{{ 
    config(
        unique_key='event_actor_pk'
    )
}}

with s_events_actors as (

    select * from {{ ref('int__events_actors') }}

),

bridge as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'action_geo_full_name'
        ]) }} as event_fk,
        {{ dbt_utils.generate_surrogate_key([
            'actor_name',
            'actor_type'
        ]) }} as actor_fk

    from s_events_actors

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'event_fk',
            'actor_fk'
        ]) }} as event_actor_pk,
        *
    
    from bridge

)

select * from final
