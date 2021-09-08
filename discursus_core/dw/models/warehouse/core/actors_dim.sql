with s_actors as (

  select * from {{ ref('int__actors') }}


),

final as (

  select
    {{ dbt_utils.surrogate_key([
      'actor_name',
      'actor_code',
      'actor_geo_country_code'
      ]) }} as actor_pk, 

    actor_name,
    actor_code,
    actor_geo_country_code,
    actor_type_group,
    actor_type_code,
    actor_type_name

  from s_actors

)

select * from final
