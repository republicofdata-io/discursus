{{ 
  config(
    unique_key='event_pk'
  )
}}

with s_events as (

  select * from {{ ref('int__events') }}

),

s_observations as (

  select * from {{ ref('observations_fct') }}

),

s_protests as (

  select * from {{ ref('protests_dim') }}

),

associate_observations as (

  select
    s_events.*,
    s_observations.*
  
  from s_events
  left join s_observations on {{ dbt_utils.surrogate_key(['s_events.gdelt_event_natural_key']) }} = s_observations.event_fk

),

associate_protests as (

  select
    associate_observations.*,
    s_protests.protest_pk
  
  from associate_observations
  left join s_protests on
    array_contains(associate_observations.action_geo_country_code::variant, split(s_protests.countries, ','))
    and associate_observations.published_date >= s_protests.published_date_start
    and associate_observations.published_date <= coalesce(s_protests.published_date_end, current_date())
    and(
      associate_observations.observation_page_description regexp s_protests.page_description_regex
      or associate_observations.observation_page_title regexp s_protests.page_description_regex
    )

),

final as (

  select distinct
    {{ dbt_utils.surrogate_key(['gdelt_event_natural_key']) }} as event_pk,
    protest_pk as protest_fk,

    gdelt_event_natural_key, 

    creation_ts, 

    action_geo_country_code,
    action_geo_country_name,
    action_geo_full_name,  
    action_geo_adm1_code, 
    action_geo_latitude, 
    action_geo_longitude, 
    event_type, 

    goldstein_scale, 
    num_mentions, 
    num_sources, 
    num_articles, 
    avg_tone

  from associate_protests

)

select * from final
