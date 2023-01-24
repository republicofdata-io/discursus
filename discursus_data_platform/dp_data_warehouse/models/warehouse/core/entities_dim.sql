{{ 
    config(
        unique_key='entity_pk',
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_entities as (

  select * from {{ ref('int__entities') }}


),

final as (

  select
    {{ dbt_utils.generate_surrogate_key([
        'published_date',
        'mention_url',
        'entity_name',
        'entity_type']) }} as entity_pk,

    published_date,
    mention_url as observation_url,
    entity_name,
    entity_type

  from s_entities

)

select * from final
