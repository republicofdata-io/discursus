{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'article_url'
    )
}}

with s_articles as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date

    from {{ source('gdelt', 'gdelt_gkg_articles') }}

    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

s_article_summaries as (

    select * from {{ ref('stg__gdelt__articles_summary') }}

),

format_fields as (

    select
        cast(gdelt_gkg_article_id as string) as gdelt_gkg_article_id,
        lower(cast(article_url as string)) as article_url,

        lower(cast(themes as string)) as themes,
        lower(cast(locations as string)) as locations,
        lower(cast(primary_location as string)) as primary_location,
        lower(cast(persons as string)) as persons,
        lower(cast(organizations as string)) as organizations,
        lower(cast(social_image_url as string)) as social_image_url,
        lower(cast(social_video_url as string)) as social_video_url,

        to_date(creation_ts) as creation_date,
        to_timestamp(creation_ts) as creation_ts,
        cast(dagster_partition_id as int) as dagster_partition_id,
        to_timestamp(bq_partition_id) as bq_partition_ts,
        source_file_date

    from s_articles

),

extract_geographical_fields as (

    select
        *,
        split_part(primary_location, '#', 2) as action_geo_full_name,
        split_part(primary_location, '#', 3) as action_geo_country_code,
        case
            when regexp_count(split_part(primary_location, '#', 2), ',') = 2 then
                trim(split_part(split_part(primary_location, '#', 2), ',', 3))
            when regexp_count(split_part(primary_location, '#', 2), ',') = 1 then
                trim(split_part(split_part(primary_location, '#', 2), ',', 2))
            else
                trim(split_part(primary_location, '#', 2))
        end as action_geo_country_name,
        case
            when regexp_count(split_part(primary_location, '#', 2), ',') = 2 then
                trim(split_part(split_part(primary_location, '#', 2), ',', 2))
            when regexp_count(split_part(primary_location, '#', 2), ',') = 1 then
                trim(split_part(split_part(primary_location, '#', 2), ',', 1))
            else ''
        end as action_geo_state_name,
        case
            when regexp_count(split_part(primary_location, '#', 2), ',') = 2 then
                trim(split_part(split_part(primary_location, '#', 2), ',', 1))
            else ''
        end as action_geo_city_name,
        cast(split_part(primary_location, '#', 5) as number(8,6)) as action_geo_latitude,
        cast(split_part(primary_location, '#', 6) as number(9,6)) as action_geo_longitude
    
    from format_fields

),

encode_h3_cells as (

    select
        *,
        analytics_toolbox.carto.h3_fromlonglat(action_geo_longitude, action_geo_latitude, 3) as action_geo_h3_r3
    
    from extract_geographical_fields

),

event_key as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'creation_date',
            'primary_location'
        ]) }} as gdelt_event_sk,
        
        gdelt_gkg_article_id,
        article_url,

        themes,
        locations,
        primary_location,
        action_geo_full_name,
        action_geo_country_code,
        action_geo_country_name,
        action_geo_state_name,
        action_geo_city_name,
        action_geo_latitude,
        action_geo_longitude,
        action_geo_h3_r3,
        persons,
        organizations,
        social_image_url,
        social_video_url,

        creation_date,
        creation_ts,
        dagster_partition_id,
        bq_partition_ts,
        source_file_date

    from encode_h3_cells

),

filter_articles as (

    select event_key.*
    from event_key
    inner join s_article_summaries using (article_url)

)

select * from filter_articles
where action_geo_city_name != ''