{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_entities as (

    select * from {{ ref('stg__gdelt__mentions_named_entities') }}

),

s_observations as (

    select * from {{ ref('int__observations') }}

),

final as (

    select
        s_entities.*,
        s_observations.published_date
    
    from s_entities
    left join s_observations on s_entities.mention_url =  s_observations.observation_url

)

select * from final
where published_date is not null