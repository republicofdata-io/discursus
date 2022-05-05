with s_gdelt_events as (

    select * from {{ ref('stg__gdelt__events') }}

),

s_actor_types as (

    select * from {{ ref('stg__seed__actor_types') }}

),

s_gdelt_ml_enriched_mentions as (

    select *
    from {{ ref('stg__gdelt__ml_enriched_mentions') }}
    where mention_url is not null
    and is_relevant

),

s_countries as (

    select * from {{ ref('stg__seed__fips_countries') }}

),

{% set partition_window = 'published_date, action_geo_latitude, action_geo_longitude' %}

final as (

    select distinct
        {{ dbt_utils.surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_sk,
       
        s_gdelt_events.published_date,

        first_value(s_gdelt_events.action_geo_full_name) over (partition by {{ partition_window }} order by s_gdelt_events.action_geo_full_name) as action_geo_full_name,
        s_gdelt_events.action_geo_country_code,
        s_countries.country_name as action_geo_country_name,
        s_gdelt_events.action_geo_latitude,
        s_gdelt_events.action_geo_longitude

    from s_gdelt_events
    left join s_actor_types as s_actor_types1
        on s_gdelt_events.actor1_type1_code = s_actor_types1.actor_type_code
    left join s_actor_types as s_actor_types2
        on s_gdelt_events.actor2_type1_code = s_actor_types2.actor_type_code
    inner join s_gdelt_ml_enriched_mentions using (mention_url)
    left join s_countries on s_gdelt_events.action_geo_country_code = s_countries.country_code

)

select * from final