{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with source as (

    select * from {{ ref('fips_country') }}

),

final as (

    select distinct
        lower(code) as country_code,
        lower(label) as country_name

    from source

)

select * from final