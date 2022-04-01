{{ 
  config(
    unique_key='observer_pk'
  )
}}

with s_observers as (

  select * from {{ ref('int__observers') }}

),

final as (

  select distinct
    {{ dbt_utils.surrogate_key(['observer_domain']) }} as observer_pk, 

    observer_domain

  from s_observers

)

select * from final
