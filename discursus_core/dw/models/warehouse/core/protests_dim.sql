{{ 
  config(
    unique_key='protest_pk'
  )
}}

with s_protests as (

  select * from {{ ref('int__protests') }}


),

final as (

  select
    {{ dbt_utils.surrogate_key(['protest_name']) }} as protest_pk, 

    protest_name,
    published_date_start,
    published_date_end,
    countries,
    page_description_regex

  from s_protests

)

select * from final
