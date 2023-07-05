{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key='observation_pk'
    )
}}

with s_observations as (

    select * from {{ ref('int__observations') }}

    {% if is_incremental() %}
        where event_date >= (select max(published_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

),

final as (

    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'observation_url'
        ]) }} as observation_pk,

        event_date as published_date,

        observation_type,
        observation_url,
        observation_page_title,
        observation_summary,
        observation_keywords,
        observation_source

    from s_observations

)

select * from final
