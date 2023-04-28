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
    from {{ source('gdelt', 'gdelt_mentions_entity_extraction') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

split_entities as (

    select distinct
        lower(cast(mention_identifier as string)) as mention_url,

        source_file_date,
        trim(lower(cast(named_entities_table.value as string))) as named_entity

    from source, lateral split_to_table(source.named_entities, ' ||') as named_entities_table

    where coalesce(trim(lower(cast(named_entities_table.value as string))), '') != ''

),

final as (

    select distinct
        mention_url,
        source_file_date,
        ltrim(cast(split(named_entity, '::')[0] as string), '(') as entity_name,
        cast(split(named_entity, '::')[1] as string) as entity_type
    
    from split_entities

)

select * from final