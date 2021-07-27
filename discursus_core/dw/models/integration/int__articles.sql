with s_gdelt_articles as (

    select * from {{ ref('stg__gdelt__mentions') }}

), 

s_gdelt_enhanced_articles as (

    select * from {{ ref('stg__gdelt__enhanced_mentions') }}

), 

final as (

    select
        gdelt_event_natural_key,

        mention_ts as article_ts,
        
        mention_source_name as article_source_name,
        mention_url as article_url,
        page_name as article_page_name, 
        file_name as article_file_name,
        page_title as article_page_title,
        page_description as article_page_description,
        keywords as article_keywords

    from s_gdelt_articles
    left join s_gdelt_enhanced_articles using (mention_url)

)

select * from final