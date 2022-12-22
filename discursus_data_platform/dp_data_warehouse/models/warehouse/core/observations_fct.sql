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
            'published_date',
            'movement_name',
            'action_geo_latitude',
            'action_geo_longitude',
            'observation_url'
        ]) }} as observation_pk, 
        {{ dbt_utils.generate_surrogate_key([
            'published_date',
            'movement_name',
            'action_geo_latitude',
            'action_geo_longitude'
        ]) }} as event_fk,

        published_date,

        observation_type,
        observation_url,
        observation_page_title,
        observation_page_description,
        observation_keywords

    from s_observations

)

select * from final
