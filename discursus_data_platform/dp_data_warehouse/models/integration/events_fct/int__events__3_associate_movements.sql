{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "event_date||'-'||movement_name",
    )
}}

with s_events as (

    select * from {{ ref('int__events__2_associate_observations') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

),

s_movements as (

    select 
        int__movements.movement_name,
        int__movements.published_date_start,
        int__movements.published_date_end,
        int__movements.countries,
        trim(a.value) as page_description_regex
        
    from {{ ref('int__movements') }},
    lateral split_to_table(int__movements.page_description_regex, ',') a

),

associate_movements as (

    select distinct
        s_events.*,
        coalesce(s_movements.movement_name, 'Other') as movement_name

    from s_events
    left join s_movements on
        array_contains(s_events.action_geo_country_code::variant, split(s_movements.countries, ','))
        and s_events.event_date >= s_movements.published_date_start
        and s_events.event_date <= coalesce(s_movements.published_date_end, current_date())
        and(
            lower(s_events.observation_summary) regexp s_movements.page_description_regex
            or lower(s_events.observation_page_title) regexp s_movements.page_description_regex
        )

),

count_movements as (

    select
        *,
        count(movement_name) over (partition by event_date, action_geo_longitude, action_geo_latitude order by movement_name) as movement_count

    from associate_movements

),

drop_others as (

    select
        * exclude movement_count
    
    from count_movements 
    where movement_count = 1 or movement_name != 'Other' -- Remove events classified as Others when there are already movements for that location and date

)

select * from drop_others