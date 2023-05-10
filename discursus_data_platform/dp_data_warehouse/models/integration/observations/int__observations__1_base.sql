with s_observations as (

    select * from {{ ref('stg__gdelt__mentions') }}

),

s_observation_summaries as (

    select * from {{ ref('stg__gdelt__mention_summaries') }}
    where mention_url is not null

),

s_observation_metadata as (

    select * from {{ ref('stg__gdelt__mentions_metadata') }}
    where mention_url is not null

),

s_observation_relevancy as (

    select 
        validation_fields,
        trim(a.value) as include_regex

    from {{ ref('stg__airbyte__observation_relevancy') }},
    lateral split_to_table(stg__airbyte__observation_relevancy.include_regex, ',') a

),

final as (

    select distinct
        s_observations.gdelt_event_natural_key,
        s_observations.mention_url as observation_url,
        s_observations.mention_time_date as published_date,

        'media article' as observation_type,
        s_observation_metadata.page_name as observation_page_name,
        s_observation_metadata.page_title as observation_page_title,
        coalesce(s_observation_summaries.summary, s_observation_metadata.page_description) as observation_summary,
        s_observation_metadata.keywords as observation_keywords

    from s_observations
    inner join s_observation_summaries using (mention_url)
    inner join s_observation_metadata using (mention_url)
    inner join s_observation_relevancy
        on (
            s_observation_metadata.page_title regexp s_observation_relevancy.include_regex
            or s_observation_metadata.page_description regexp s_observation_relevancy.include_regex
        )

)

select * from final