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

    from {{ source('gdelt', 'gdelt_articles_enhanced') }}

    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

final as (

    select
        lower(cast(article_url as string)) as article_url,

        lower(cast(file_name as string)) as file_name,
        lower(cast(page_title as string)) as page_title,
        lower(cast(page_description as string)) as page_description,
        lower(cast(keywords as string)) as keywords,

        source_file_date

    from source

)


select * from final