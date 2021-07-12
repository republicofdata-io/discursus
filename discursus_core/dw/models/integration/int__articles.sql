with s_gdelt_articles as (

    select * from {{ ref('stg__gdelt__mentions') }}

), 

final as (

    select
        gdelt_event_natural_key,

        mention_ts as article_ts,
        
        mention_source_name as article_source_name,
        mention_url as article_url

    from s_gdelt_articles

)

select * from final