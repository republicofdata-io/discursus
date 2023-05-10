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
    from {{ source('gdelt', 'gdelt_mention_summaries') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

final as (

    select distinct
        case
            when lower(cast(mention_identifier as string)) like '://%www.msn.com%' then regexp_replace(lower(cast(mention_identifier as string)), '/[^/]*$', '/')
            else lower(cast(mention_identifier as string))
        end as mention_url,
        summary,
        source_file_date

    from source

)

select * from final