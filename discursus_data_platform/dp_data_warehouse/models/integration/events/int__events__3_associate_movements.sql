with s_events as (

    select * from {{ ref('int__events__2_associate_observations') }}

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

final as (

    select distinct
        s_events.*,
        coalesce(s_movements.movement_name, 'Other') as movement_name

    from s_events
    left join s_movements on
        array_contains(s_events.action_geo_country_code::variant, split(s_movements.countries, ','))
        and s_events.published_date >= s_movements.published_date_start
        and s_events.published_date <= coalesce(s_movements.published_date_end, current_date())
        and(
            s_events.observation_page_description regexp s_movements.page_description_regex
            or s_events.observation_page_title regexp s_movements.page_description_regex
        )

)

select * from final