with s_gdelt_events as (

    select
        {{ dbt_utils.surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_sk,
        *

    from {{ ref('stg__gdelt__events') }}
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
        s_gdelt_ml_enriched_mentions.mention_url as observation_url,
        s_gdelt_events.published_date,

        'media article' as observation_type,
        s_gdelt_ml_enriched_mentions.page_name as observation_page_name,
        s_gdelt_ml_enriched_mentions.file_name as observation_file_name,
        s_gdelt_ml_enriched_mentions.page_title as observation_page_title,
        s_gdelt_ml_enriched_mentions.page_description as observation_page_description,
        s_gdelt_ml_enriched_mentions.keywords as observation_keywords

    from s_gdelt_events
    inner join s_gdelt_ml_enriched_mentions using (mention_url)

)

select * from final