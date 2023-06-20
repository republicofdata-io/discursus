{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'article_url'
    )
}}

with source as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date

    from {{ source('gdelt', 'gdelt_article_summaries') }}

    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

format_fields as (

    select
        lower(cast(article_identifier as string)) as article_url,

        cast(summary as string) as summary,

        source_file_date

    from source

),

filter_protest_articles as (

    select
        *,
        case
            when summary like '%protest%' then true
            when summary like '%manifest%' then true
            else false
        end as is_protest_article

    from format_fields

)

select * from filter_protest_articles
where is_protest_article