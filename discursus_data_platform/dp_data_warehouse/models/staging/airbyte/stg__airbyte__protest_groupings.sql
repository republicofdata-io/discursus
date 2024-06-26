{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 24*60},
        dagster_auto_materialize_policy = {"type": "eager"},
    )
}}

with source as (

    select * from {{ source('airbyte', 'protest_groupings') }}

),

base as (

    select
        cast(protest_name as varchar) as movement_name,
        cast(published_date_start as date) as published_date_start,
        cast(published_date_end as date) as published_date_end,
        cast(countries as varchar) as countries,
        cast(page_description as varchar) as page_description_regex
    
    from source

)

select * from base