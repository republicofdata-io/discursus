with s_gdelt_events as (

    select * from {{ ref('stg__gdelt__events') }}
    where mention_url is not null

),

s_gdelt_enhanced_articles as (

    select * from {{ ref('stg__gdelt__enhanced_mentions') }}
    where mention_url is not null

),

s_gdelt_ml_enriched_mentions as (

    select * from {{ ref('stg__gdelt__ml_enriched_mentions') }}
    where mention_url is not null
    and is_relevant

),

final as (

    select distinct
        s_gdelt_events.gdelt_event_natural_key,
        s_gdelt_enhanced_articles.mention_url as article_url,

        s_gdelt_events.published_date,
        s_gdelt_events.creation_ts,

        'media article' as observation_type,
        s_gdelt_enhanced_articles.page_name as article_page_name,
        s_gdelt_enhanced_articles.file_name as article_file_name,
        s_gdelt_enhanced_articles.page_title as article_page_title,
        s_gdelt_enhanced_articles.page_description as article_page_description,
        s_gdelt_enhanced_articles.keywords as article_keywords

    from s_gdelt_events
    inner join s_gdelt_enhanced_articles using (mention_url)
    inner join s_gdelt_ml_enriched_mentions using (mention_url)

)

select * from final