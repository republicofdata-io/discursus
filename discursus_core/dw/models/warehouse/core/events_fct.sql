{{ 
  config(
    unique_key='event_pk'
  )
}}

with s_events as (

  select * from {{ ref('int__events') }}

),

add_actor_type_combo as (

  select
    *,
    case
      when actor1_code < actor2_code then actor1_code || ' - ' || actor2_code
      else actor2_code || ' - ' || actor1_code
    end as actor_type_combo

  
  from s_events

),

final as (

  select
    {{ dbt_utils.surrogate_key(['gdelt_event_natural_key']) }} as event_pk,
    {{ dbt_utils.surrogate_key([
      'action_geo_country_code',
      'action_geo_adm1_code',
      'actor_type_combo',
      'event_type'
    ]) }} as protest_sk,
    case 
      when action_geo_country_code is null then null
      else {{ dbt_utils.surrogate_key(['action_geo_country_code']) }} 
    end as country_fk,
    {{ dbt_utils.surrogate_key([
      'actor1_name',
      'actor1_code',
      'actor1_geo_country_code'
    ]) }} as actor1_fk,
    {{ dbt_utils.surrogate_key([
      'actor2_name',
      'actor2_code',
      'actor2_geo_country_code'
    ]) }} as actor2_fk,

    gdelt_event_natural_key, 

    published_date, 
    creation_ts, 

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

  from add_actor_type_combo

)

select * from final
