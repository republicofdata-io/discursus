with source as (

    select * from {{ source('gdelt', 'gdelt_mentions') }}

),

final as (

    select distinct
        cast(gdelt_id as bigint) as gdelt_event_natural_key,

        to_timestamp(cast(event_time_date as string), 'YYYYMMDDHH24MISS') as event_ts,
        to_timestamp(cast(mention_time_date as string), 'YYYYMMDDHH24MISS') as mention_ts,
        cast(mention_type as integer) as mention_type,
        lower(cast(mention_source_name as string)) as mention_source_name,
        lower(cast(mention_identifier as string)) as mention_url,
        cast(sentence_id as integer) as sentence_id,
        cast(actor1_char_offset as integer) as actor1_char_offset,
        cast(actor2_char_offset as integer) as actor2_char_offset,
        cast(action_char_offset as integer) as action_char_offset,
        cast(in_raw_text as integer) as in_raw_text,
        cast(confidence as integer) as confidence,
        cast(mention_doc_len as integer) as mention_doc_len,
        cast(mention_doc_tone as integer) as mention_doc_tone,
        cast(mention_doc_translation_info as string) as mention_doc_translation_info

    from source

)

select * from final

where event_ts >= dateadd(day, -90, current_date)
    and mention_type = 1
    and in_raw_text = 1
    and confidence > 50