{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key='event_pk'
    )
}}

with s_events as (

    select * from {{ ref('int__events') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

),

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'movement_name',
            'action_geo_country_name',
            'action_geo_state_name',
            'action_geo_city_name'
        ]) }} as event_pk, 
        {{ dbt_utils.generate_surrogate_key([
            'movement_name'
        ]) }} as movement_fk, 

        event_date, 
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        action_geo_state_name,
        action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude,
        action_geo_h3_r3

    from s_events

)

select * from final
