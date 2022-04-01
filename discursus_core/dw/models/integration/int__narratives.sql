with s_events as (

  select * from {{ ref('int__events') }}

),

first_actors as (

  select
    gdelt_event_natural_key,
    actor1_name as actor_name,
    actor1_code as actor_code,
    actor1_geo_country_code as actor_geo_country_code

  from s_events

),

second_actors as (

  select
    gdelt_event_natural_key,
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
    gdelt_event_natural_key,
    actor_name,
    actor_code,
    actor_geo_country_code,
    null as narrative

  from merge_actors

)

select * from final