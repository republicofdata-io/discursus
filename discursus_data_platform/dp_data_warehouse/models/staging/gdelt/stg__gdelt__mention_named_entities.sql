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
    from {{ source('gdelt', 'gdelt_mention_named_entities') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

base as (

    select distinct
        case
            when lower(cast(mention_identifier as string)) like '://%www.msn.com%' then regexp_replace(lower(cast(mention_identifier as string)), '/[^/]*$', '/')
            else lower(cast(mention_identifier as string))
        end as mention_url,
        named_entities,
        source_file_date

    from source

),

final as (

    select distinct
        mention_url,
        source_file_date,
        trim(lower(cast(entities.value as string))) as entity_name

    from base, lateral flatten(input => parse_json(named_entities)) as entities

    where coalesce(trim(lower(cast(entities.value as string))), '') != ''

)

select * from final