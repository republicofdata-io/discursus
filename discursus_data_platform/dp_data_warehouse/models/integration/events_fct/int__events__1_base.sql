with s_gdelt_events as (

    select * from {{ ref('stg__gdelt__events') }}

),

s_countries as (

    select * from {{ ref('stg__seed__fips_countries') }}

),

{% set partition_window = 'published_date, action_geo_latitude, action_geo_longitude' %}

get_unique_geo as (

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

),

extract_state_city as (

    select distinct
        gdelt_event_natural_key,
        published_date,
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        case
            when regexp_count(action_geo_full_name, ',') = 1 then trim(split_part(action_geo_full_name, ',', 1))
            when regexp_count(action_geo_full_name, ',') = 2 then trim(split_part(action_geo_full_name, ',', 2))
            else ''
        end as action_geo_state_name,
        case
            when regexp_count(action_geo_full_name, ',') = 2 then trim(split_part(action_geo_full_name, ',', 1))
            else ''
        end as action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude

    from get_unique_geo

),

final as (

    select * from extract_state_city

)

select * from final