{{ 
  config(
    unique_key='movement_pk'
  )
}}

with s_movements as (

  select * from {{ ref('int__movements') }}


),

final as (

  select
    {{ dbt_utils.surrogate_key(['movement_name']) }} as movement_pk, 

    movement_name,
    published_date_start,
    published_date_end,
    countries,
    page_description_regex

  from s_movements

)

select * from final
