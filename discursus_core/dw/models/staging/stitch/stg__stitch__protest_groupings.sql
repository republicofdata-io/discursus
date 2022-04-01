with source as (

    select * from {{ source('stitch', 'protest_groupings') }}

),

base as (

    select
        cast(protest_name as varchar) as protest_name,
        cast(published_date_start as date) as published_date_start,
        cast(published_date_end as date) as published_date_end,
        cast(countries as varchar) as countries,
        cast(page_description as varchar) as page_description_regex
    
    from source

)

select * from base