{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'gdelt_event_sk',
        dagster_auto_materialize_policy = {"type": "lazy"},
        dagster_freshness_policy = {"maximum_lag_minutes": 60 * 24 * 7},
    )
}}

with s_gdelt_events as (

    select * from {{ source('gdelt', 'gdelt_events') }}
    
    {% if is_incremental() %}
        where to_timestamp(cast(date_added as string), 'YYYYMMDDHH24MISS') >= (select max(creation_ts) from {{ this }})
    {% else %}
        where to_timestamp(cast(date_added as string), 'YYYYMMDDHH24MISS') >= dateadd(week, -52, current_date)
    {% endif %}

    and event_root_code = '14'
    and cast(action_geo_lat as number(8,6)) is not null
    and cast(action_geo_long as number(9,6)) is not null
    and lower(cast(action_geo_country_code as string)) in ('us', 'ca')

),

s_countries as (

    select * from {{ ref('stg__seed__fips_countries') }}

),

format_fields as (

    select distinct
        cast(gdelt_id as string) as gdelt_event_sk,

        to_timestamp(cast(date_added as string), 'YYYYMMDDHH24MISS') as creation_ts,
        to_date(
            case when published_date = '' then null else published_date end,
            'yyyymmdd'
        ) as published_date,
        cast(
            case when published_date_my = '' then null else published_date_my end
            as integer
        ) as published_date_my,
        cast(
            case when published_date_year = '' then null else published_date_year end
            as integer
        ) as published_date_year,
        cast(
            case when published_date_fraction_date = '' then null else published_date_fraction_date	end
            as numeric(8,4)
        ) as published_date_fraction_date,

        cast(url as string) as source_url,
        cast(is_root_event as integer) as is_root_event,
        lower(cast(event_code as string)) as event_code,
        lower(cast(event_base_code as string)) as event_base_code,
        lower(cast(event_root_code as string)) as event_root_code,

        lower(cast(actor1_code as string)) as actor1_code,
        lower(cast(actor1_name as string)) as actor1_name,
        lower(cast(actor1_country_code as string)) as actor1_country_code,
        lower(cast(actor1_known_group_code as string)) as actor1_known_group_code,
        lower(cast(actor1_ethnic_code as string)) as actor1_ethnic_code,
        lower(cast(actor1_religion1_code as string)) as actor1_religion1_code,
        lower(cast(actor1_religion2_code as string)) as actor1_religion2_code,
        lower(cast(actor1_type1_code as string)) as actor1_type1_code,
        lower(cast(actor1_type2_code as string)) as actor1_type2_code,
        lower(cast(actor1_type3_code as string)) as actor1_type3_code,
        cast(actor1_geo_type as integer) as actor1_geo_type,
        lower(cast(actor1_geo_full_name as string)) as actor1_geo_full_name,
        lower(cast(actor1_geo_country_code as string)) as actor1_geo_country_code,
        lower(cast(actor1_geo_adm1_code as string)) as actor1_geo_adm1_code,
        lower(cast(actor1_geo_adm2_code as string)) as actor1_geo_adm2_code,
        cast(actor1_geo_lat as number(8,6)) as actor1_geo_latitude,
        cast(actor1_geo_long as number(9,6)) as actor1_geo_longitude,
        lower(cast(actor1_geo_feature_id as string)) as actor1_geo_feature_id,

        lower(cast(actor2_code as string)) as actor2_code,
        lower(cast(actor2_name as string)) as actor2_name,
        lower(cast(actor2_country_code as string)) as actor2_country_code,
        lower(cast(actor2_known_group_code as string)) as actor2_known_group_code,
        lower(cast(actor2_ethnic_code as string)) as actor2_ethnic_code,
        lower(cast(actor2_religion1_code as string)) as actor2_religion1_code,
        lower(cast(actor2_religion2_code as string)) as actor2_religion2_code,
        lower(cast(actor2_type1_code as string)) as actor2_type1_code,
        lower(cast(actor2_type2_code as string)) as actor2_type2_code,
        lower(cast(actor2_type3_code as string)) as actor2_type3_code,
        cast(actor2_geo_type as integer) as actor2_geo_type,
        lower(cast(actor2_geo_full_name as string)) as actor2_geo_full_name,
        lower(cast(actor2_geo_country_code as string)) as actor2_geo_country_code,
        lower(cast(actor2_geo_adm1_code as string)) as actor2_geo_adm1_code,
        lower(cast(actor2_geo_adm2_code as string)) as actor2_geo_adm2_code,
        cast(actor2_geo_lat as number(8,6)) as actor2_geo_latitude,
        cast(actor2_geo_long as number(9,6)) as actor2_geo_longitude,
        lower(cast(actor2_geo_feature_id as string)) as actor2_geo_feature_id,

        cast(action_geo_type as integer) as action_geo_type,
        lower(cast(action_geo_full_name as string)) as action_geo_full_name,
        lower(cast(action_geo_country_code as string)) as action_geo_country_code,
        lower(cast(action_geo_adm1_code as string)) as action_geo_adm1_code,
        lower(cast(action_geo_adm2_code as string)) as action_geo_adm2_code,
        cast(action_geo_lat as number(8,6)) as action_geo_latitude,
        cast(action_geo_long as number(9,6)) as action_geo_longitude,
        lower(cast(action_geo_feature_id as string)) as action_geo_feature_id,

        cast(quad_class as integer) as quad_class,
        cast(goldstein_scale as real) as goldstein_scale,
        cast(num_mentions as integer) as num_mentions,
        cast(num_sources as integer) as num_sources,
        cast(num_articles as integer) as num_articles,
        cast(avg_tone as real) as avg_tone,

        case
            when lower(cast(url as string)) like '://%www.msn.com%' then regexp_replace(lower(cast(url as string)), '/[^/]*$', '/')
            else lower(cast(url as string))
        end as mention_url


    from s_gdelt_events

),

{% set partition_window = 'published_date, action_geo_latitude, action_geo_longitude' %}

get_unique_geo as (

    select distinct
        format_fields.gdelt_event_sk,
        format_fields.creation_ts,
        format_fields.published_date,
        first_value(format_fields.action_geo_full_name) over (partition by {{ partition_window }} order by format_fields.action_geo_full_name) as action_geo_full_name,
        format_fields.action_geo_country_code,
        s_countries.country_name as action_geo_country_name,
        format_fields.action_geo_latitude,
        format_fields.action_geo_longitude

    from format_fields
    left join s_countries on format_fields.action_geo_country_code = s_countries.country_code

),

extract_state_city as (

    select distinct
        gdelt_event_sk,
        creation_ts,
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

encode_h3_cells as (

    select
        *,
        analytics_toolbox.carto.h3_fromlonglat(action_geo_longitude, action_geo_latitude, 3) as action_geo_h3_r3
    
    from extract_state_city

)

select * from encode_h3_cells
where action_geo_city_name != ''