{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'mention_url',
        dagster_freshness_policy = {"maximum_lag_minutes": 6 * 60}
    )
}}

with source as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date
    
    from {{ source('gdelt', 'gdelt_mentions_enhanced') }}
    
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

    and (page_title is not null or page_description is not null)

),

final as (

    select distinct
        lower(cast(mention_identifier as string)) as mention_url,

        source_file_date,
        lower(cast(page_name as string)) as page_name,
        lower(cast(file_name as string)) as file_name,
        lower(cast(page_title as string)) as page_title,
        lower(cast(page_description as string)) as page_description,
        lower(cast(keywords as string)) as keywords

    from source

)

select * from final