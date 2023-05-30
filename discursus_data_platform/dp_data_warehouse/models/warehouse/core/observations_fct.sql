{{ 
    config(
        unique_key='observation_pk'
    )
}}

with s_observations as (

    select * from {{ ref('int__observations') }}

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
