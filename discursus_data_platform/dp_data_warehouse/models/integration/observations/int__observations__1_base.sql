{{
    config(
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with s_gdelt_events as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_sk,
        *

    from {{ ref('stg__gdelt__events') }}
    where mention_url is not null

),

s_gdelt_mentions_relevancy as (

    select * from {{ ref('stg__gdelt__mentions_relevancy') }}
    where mention_url is not null
    and is_relevant

),

final as (

    select distinct
        s_gdelt_events.gdelt_event_natural_key,
        s_gdelt_mentions_relevancy.mention_url as observation_url,
        s_gdelt_events.published_date,

        'media article' as observation_type,
        s_gdelt_mentions_relevancy.page_name as observation_page_name,
        s_gdelt_mentions_relevancy.file_name as observation_file_name,
        s_gdelt_mentions_relevancy.page_title as observation_page_title,
        s_gdelt_mentions_relevancy.page_description as observation_page_description,
        s_gdelt_mentions_relevancy.keywords as observation_keywords

    from s_gdelt_events
    inner join s_gdelt_mentions_relevancy using (mention_url)

)

select * from final