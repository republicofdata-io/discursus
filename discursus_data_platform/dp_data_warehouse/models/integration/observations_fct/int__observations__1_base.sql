{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "gdelt_event_sk",
    )
}}

with s_articles as (

    select * from {{ ref('stg__gdelt__articles') }}

    {% if is_incremental() %}
        where source_file_date >= (select max(published_date) from {{ this }})
    {% else %}
        where source_file_date >= dateadd(week, -52, source_file_date)
    {% endif %}

),

s_article_summaries as (

    select * from {{ ref('stg__gdelt__articles_summary') }}

    {% if is_incremental() %}
        where source_file_date >= (select max(published_date) from {{ this }})
    {% else %}
        where source_file_date >= dateadd(week, -52, source_file_date)
    {% endif %}

),

s_article_enhanced as (

    select * from {{ ref('stg__gdelt__articles_enhanced') }}

    {% if is_incremental() %}
        where source_file_date >= (select max(published_date) from {{ this }})
    {% else %}
        where source_file_date >= dateadd(week, -52, source_file_date)
    {% endif %}

),

articles as (

    select distinct
        s_articles.gdelt_event_sk,
        s_articles.article_url as observation_url,
        s_articles.creation_date as published_date,

        'media article' as observation_type,
        s_article_enhanced.page_title as observation_page_title,
        coalesce(s_article_summaries.summary, s_article_enhanced.page_description) as observation_summary,
        s_article_enhanced.keywords as observation_keywords,
        'gdelt' as observation_source

    from s_articles
    left join s_article_summaries using (article_url)
    inner join s_article_enhanced using (article_url)

),

dedup_articles as (

    select
        *,
        row_number() over (partition by observation_page_title order by published_date) as row_number

    from articles

)

select * from dedup_articles
where row_number = 1