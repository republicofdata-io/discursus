with s_actor_types as (

    select * from {{ ref('stg__seed__actor_types') }}

),

s_actors as (

    select
        actor1_name as actor_name,
        actor1_code as actor_code,
        actor1_geo_country_code as actor_geo_country_code,
        actor1_type1_code as actor_type_code,
        s_actor_type1.actor_type as actor_type_name

    from {{ ref('stg__gdelt__events') }}
    left join s_actor_types as s_actor_type1
        on actor1_type1_code = s_actor_type1.actor_type_code
    left join s_actor_types as s_actor_type2
        on actor1_type2_code = s_actor_type2.actor_type_code
    left join s_actor_types as s_actor_type3
        on actor1_type3_code = s_actor_type3.actor_type_code

    union all

    select
        actor2_name as actor_name,
        actor2_code as actor_code,
        actor2_geo_country_code as actor_geo_country_code,
        actor2_type1_code as actor_type_code,
        s_actor_type1.actor_type as actor_type_name

    from {{ ref('stg__gdelt__events') }}
    left join s_actor_types as s_actor_type1
        on actor2_type1_code = s_actor_type1.actor_type_code
    left join s_actor_types as s_actor_type2
        on actor2_type2_code = s_actor_type2.actor_type_code
    left join s_actor_types as s_actor_type3
        on actor2_type3_code = s_actor_type3.actor_type_code

),

final as (

    select distinct
        actor_name,
        actor_code,
        actor_geo_country_code,
        actor_type_code,
        actor_type_name

    from s_actors

)

select * from final