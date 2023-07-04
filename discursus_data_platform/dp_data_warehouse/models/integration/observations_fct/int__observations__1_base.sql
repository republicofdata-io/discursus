with s_historical_mentions as (

    select * from {{ ref('stg__gdelt__mentions') }}

),

s_historical_mention_summaries as (

    select * from {{ ref('stg__gdelt__mention_summaries') }}
    where mention_url is not null

),

s_historical_mention_metadata as (

    select * from {{ ref('stg__gdelt__mentions_metadata') }}
    where mention_url is not null

),

s_articles as (

    select * from {{ ref('stg__gdelt__articles') }}

),

s_article_summaries as (

    select * from {{ ref('stg__gdelt__articles_summary') }}

),

s_article_enhanced as (

    select * from {{ ref('stg__gdelt__articles_enhanced') }}

),

historical_mentions as (

    select distinct
        cast(s_historical_mentions.gdelt_event_natural_key as string) as gdelt_event_sk,
        s_historical_mentions.mention_url as observation_url,
        s_historical_mentions.mention_time_date as published_date,

        'media article' as observation_type,
        s_historical_mention_metadata.page_title as observation_page_title,
        coalesce(s_historical_mention_summaries.summary, s_historical_mention_metadata.page_description) as observation_summary,
        s_historical_mention_metadata.keywords as observation_keywords,
        'gdelt historical' as observation_source

    from s_historical_mentions
    left join s_historical_mention_summaries using (mention_url)
    inner join s_historical_mention_metadata using (mention_url)

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

merge_sources as (

    select * from historical_mentions
    union all
    select * from articles

),

dedup_articles as (

    select
        *,
        row_number() over (partition by observation_page_title order by published_date) as row_number

    from merge_sources

)

select * from dedup_articles
where row_number = 1