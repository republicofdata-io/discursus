{{ 
    config(
        unique_key='actor_pk'
    )
}}

with s_actors as (

  select * from {{ ref('int__actors') }}


),

final as (

  select
    {{ dbt_utils.generate_surrogate_key(['actor_name']) }} as actor_pk, 

    actor_name

  from s_actors

)

select * from final