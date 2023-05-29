{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'article_url'
    )
}}

with s_enhanced_articles as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date

    from {{ source('gdelt', 'gdelt_articles_enhanced') }}

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
        lower(cast(article_url as string)) as article_url,

        lower(cast(file_name as string)) as file_name,
        lower(cast(page_title as string)) as page_title,
        lower(cast(page_description as string)) as page_description,
        lower(cast(keywords as string)) as keywords,

        source_file_date

    from s_enhanced_articles

),

filter_articles as (

    select format_fields.*
    from format_fields
    inner join s_article_summaries using (article_url)

)

select * from filter_articles