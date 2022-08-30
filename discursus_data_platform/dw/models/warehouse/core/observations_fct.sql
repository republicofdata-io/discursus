{{ 
    config(
        unique_key='observation_pk'
    )
}}

with s_articles as (

    select
        *,
        split(split(article_url, '//')[1], '/')[0]::string as observer_domain

    from {{ ref('int__articles') }}

),

s_events as (

    select
        {{ dbt_utils.surrogate_key([
            'published_date',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_sk,
        * 
    
    from {{ ref('int__events') }}

),

filter_observations as (

    select 
        s_articles.*

    from s_articles
    inner join s_events using (event_sk)

),

final as (

    select distinct
        {{ dbt_utils.surrogate_key([
            'event_sk',
            'article_url'
        ]) }} as observation_pk, 
        event_sk as event_fk,
        {{ dbt_utils.surrogate_key(['observer_domain']) }} as observer_fk,

        published_date,

        observation_type,
        article_url as observation_url,
        article_page_title as observation_page_title,
        article_page_description as observation_page_description,
        article_keywords as observation_keywords

    from filter_observations

)

select * from final
