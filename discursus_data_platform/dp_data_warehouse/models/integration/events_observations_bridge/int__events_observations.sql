{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = "event_date||'-'||movement_name||'-'||observation_url",
    )
}}

with s_observations as (

    select * from {{ ref('int__observations') }}

    {% if is_incremental() %}
        where event_date >= (select max(event_date) from {{ this }})
    {% else %}
        where event_date >= dateadd(week, -52, event_date)
    {% endif %}

)

select * from s_observations
