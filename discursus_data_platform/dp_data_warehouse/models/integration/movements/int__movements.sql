with s_protest_groupings as (

    select * from {{ ref('stg__airbyte__protest_groupings') }}

),

add_other as (

    select 
        movement_name,
        published_date_start,
        published_date_end,
        countries,
        page_description_regex
    
    from s_protest_groupings

    union all

    select
        'Other' as movement_name,
        date('2021-01-01') as published_date_start,
        null as published_date_end,
        'ca,us' as countries,
        null as page_description_regex

)

select * from add_other