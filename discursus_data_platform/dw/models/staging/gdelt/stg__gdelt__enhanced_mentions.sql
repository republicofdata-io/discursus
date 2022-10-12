{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'mention_url'
    )
}}

with source as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date
    from {{ source('gdelt', 'gdelt_enhanced_mentions') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% endif %}

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
where source_file_date >= dateadd(week, -26, current_date)
and page_title not regexp 'opinion.*'
and page_title not regexp 'op-ed.*'
and page_title not regexp 'column.*'
and page_title not regexp 'editorial.*'
and page_title not regexp 'letter.*'