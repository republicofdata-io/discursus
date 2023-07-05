{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key='event_observation_pk'
    )
}}

with s_observations as (

    select * from {{ ref('int__events_observations') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

),

bridge as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'observation_url'
        ]) }} as observation_fk, 
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'movement_name',
            'action_geo_country_name',
            'action_geo_state_name',
            'action_geo_city_name'
        ]) }} as event_fk,
        event_date

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
