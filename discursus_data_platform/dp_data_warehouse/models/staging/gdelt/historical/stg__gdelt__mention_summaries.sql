{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'mention_url',
        dagster_auto_materialize_policy = {"type": "lazy"},
    )
}}

with source as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date
    from {{ source('gdelt', 'gdelt_mention_summaries') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

clean_up as (

    select distinct
        case
            when lower(cast(mention_identifier as string)) like '://%www.msn.com%' then regexp_replace(lower(cast(mention_identifier as string)), '/[^/]*$', '/')
            else lower(cast(mention_identifier as string))
        end as mention_url,
        summary,
        source_file_date

    from source

),

dedup as (

    select distinct
        mention_url,
        first_value(summary) over (partition by mention_url order by source_file_date desc) as summary,
        source_file_date

    from clean_up


),

filter_protest_articles as (

    select
        *,
        case
            when summary like '%protest%' then true
            when summary like '%manifest%' then true
            else false
        end as is_protest_article

    from dedup

)

select * from filter_protest_articles
where is_protest_article