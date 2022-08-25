{{
    config(
        materialized = 'incremental'
    )
}}

with source as (

    select
        *,
        date(split(metadata_filename, '/')[2], 'yyyymmdd') as source_file_date
    from {{ source('gdelt', 'gdelt_mentions') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') > (select max(source_file_date) from {{ this }})
    {% endif %}

),

final as (

    select distinct
        lower(cast(mention_identifier as string)) as mention_url,
        cast(gdelt_id as bigint) as gdelt_event_natural_key,

        source_file_date,
        cast(event_time_date as date) as event_time_date,
        cast(mention_time_date as date) as mention_time_date,
        lower(cast(mention_type as string)) as mention_type,
        lower(cast(mention_source_name as string)) as mention_source_name,
        lower(cast(sentence_id as int)) as sentence_id,
        lower(cast(actor1_char_offset as int)) as actor1_char_offset,
        lower(cast(actor2_char_offset as int)) as actor2_char_offset,
        lower(cast(action_char_offset as int)) as action_char_offset,
        lower(cast(in_raw_text as int)) as in_raw_text,
        lower(cast(confidence as string)) as confidence,
        lower(cast(mention_doc_len as string)) as mention_doc_len,
        lower(cast(mention_doc_tone as float)) as mention_doc_tone,
        lower(cast(mention_doc_translation_info as string) as mention_doc_translation_info)

    from source

)

select * from final
where source_file_date >= dateadd(week, -26, current_date)