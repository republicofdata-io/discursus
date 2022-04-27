with s_events as (

    select {{ dbt_utils.surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_sk,
        * 
    
    from {{ ref('stg__gdelt__events') }}

),

first_actors as (

    select
        event_sk,
        actor1_name as actor_name,
        actor1_code as actor_code,
        actor1_geo_country_code as actor_geo_country_code

    from s_events

),

second_actors as (

    select
        event_sk,
        actor2_name as actor_name,
        actor2_code as actor_code,
        actor2_geo_country_code as actor_geo_country_code

    from s_events

),

merge_actors as (

    select * from first_actors
    union all
    select * from second_actors

),

final as (

    select distinct
        event_sk,
        actor_name,
        actor_code,
        actor_geo_country_code,
        null as narrative

    from merge_actors

)

select * from final