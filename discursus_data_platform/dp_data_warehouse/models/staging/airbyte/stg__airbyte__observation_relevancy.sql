{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with source as (

    select * from {{ source('airbyte', 'observation_relevancy') }}

),

base as (

    select
        cast(validation_fields as varchar) as validation_fields,
        cast(include_regex as varchar) as include_regex
    
    from source

)

select * from base