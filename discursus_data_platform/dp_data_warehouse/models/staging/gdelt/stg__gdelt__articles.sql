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

    from {{ source('gdelt', 'gdelt_articles') }}

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

event_key as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'creation_date',
            'primary_location'
        ]) }} as gdelt_event_sk,
        *

    from format_fields

),

filter_articles as (

    select event_key.*
    from event_key
    inner join s_article_summaries using (article_url)

)

select * from filter_articles