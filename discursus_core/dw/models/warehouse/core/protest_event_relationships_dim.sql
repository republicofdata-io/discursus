{{ 
  config(
    unique_key='event_pk'
  )
}}

with s_events as (

  select * from {{ ref('events_fct') }}

),

s_articles as (

  select * from {{ ref('articles_fct') }}

),

s_protests as (

  select * from {{ ref('protests_dim') }}

),

associate_articles as (

  select
    s_events.*,
    s_articles.*
  
  from s_events
  inner join s_articles on s_events.event_pk = s_articles.event_fk

),

associate_protests as (

  select
    associate_articles.*,
    s_protests.protest_pk
  
  from associate_articles
  inner join s_protests on
    array_contains(associate_articles.action_geo_country_code::variant, split(s_protests.countries, ','))
    and associate_articles.published_date >= s_protests.published_date_start
    and associate_articles.published_date <= coalesce(s_protests.published_date_end, current_date())
    and(
      associate_articles.article_page_description regexp s_protests.page_description
      or associate_articles.article_page_title regexp s_protests.page_description
    )

),

final as (

  select distinct
    {{ dbt_utils.surrogate_key([
      'protest_pk',
      'event_pk'
    ]) }} as protest_event_relationship_pk,
    protest_pk as protest_fk,
    event_pk as event_fk

  from associate_protests

)

select * from final
