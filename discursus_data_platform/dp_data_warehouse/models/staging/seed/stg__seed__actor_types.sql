{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with source as (

    select * from {{ ref('actor_types') }}

),

final as (

    select distinct
        lower(code) as actor_type_code,
        lower(label) as actor_type

    from source

)

select * from final