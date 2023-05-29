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
        case
            when lower(cast(mention_identifier as string)) like '://%www.msn.com%' then regexp_replace(lower(cast(mention_identifier as string)), '/[^/]*$', '/')
            else lower(cast(mention_identifier as string))
        end as mention_url,

        source_file_date,
        lower(cast(page_name as string)) as page_name,
        lower(cast(file_name as string)) as file_name,
        case
            when trim(page_title::string) = '' then null
            else lower(page_title::string)
        end as page_title,
        case
            when trim(page_description::string) = '' then null
            else lower(page_description::string)
        end as page_description,
        case
            when trim(coalesce(keywords::string, '')) = '' then null
            else lower(keywords::string)
        end as keywords

    from source

)

select * from final