with s_gdelt_events as (

    select * from {{ ref('stg__gdelt__events') }}

),

s_countries as (

    select * from {{ ref('stg__seed__fips_countries') }}

),

{% set partition_window = 'published_date, action_geo_latitude, action_geo_longitude' %}

final as (

    select distinct
        s_gdelt_events.gdelt_event_natural_key,
        s_gdelt_events.published_date,
        first_value(s_gdelt_events.action_geo_full_name) over (partition by {{ partition_window }} order by s_gdelt_events.action_geo_full_name) as action_geo_full_name,
        s_gdelt_events.action_geo_country_code,
        s_countries.country_name as action_geo_country_name,
        s_gdelt_events.action_geo_latitude,
        s_gdelt_events.action_geo_longitude

    from s_gdelt_events
    left join s_countries on s_gdelt_events.action_geo_country_code = s_countries.country_code

)

select * from final