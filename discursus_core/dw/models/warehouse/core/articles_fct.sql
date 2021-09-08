{{ 
  config(
    unique_key='article_pk'
  )
}}

with s_articles as (

  select * from {{ ref('int__articles') }}

),

s_events as (

  select * from {{ ref('events_fct') }}

),

final as (

  select distinct
    {{ dbt_utils.surrogate_key([
        's_articles.gdelt_event_natural_key', 
        's_articles.article_url'
      ]) }} as article_pk, 
    {{ dbt_utils.surrogate_key(['s_articles.gdelt_event_natural_key']) }} as event_fk, 

    s_articles.gdelt_event_natural_key, 
    s_articles.article_url, 

    s_articles.article_ts,

    s_articles.article_source_name,
    s_articles.article_page_name,
    s_articles.article_file_name,
    s_articles.article_page_title,
    s_articles.article_page_description,
    s_articles.article_keywords

  from s_articles
  inner join s_events using (gdelt_event_natural_key)

)

select * from final
