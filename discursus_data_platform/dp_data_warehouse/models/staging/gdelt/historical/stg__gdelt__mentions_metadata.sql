{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'mention_url',
    )
}}

with s_mention_metadata as (

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

s_mention_summaries as (

    select * from {{ ref('stg__gdelt__mention_summaries') }}

),

format_fields as (

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

    from s_mention_metadata

),

filter_articles as (

    select format_fields.*
    from format_fields
    inner join s_mention_summaries using (mention_url)

)

select * from filter_articles