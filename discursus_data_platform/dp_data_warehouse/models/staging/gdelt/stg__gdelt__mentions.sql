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
    from {{ source('gdelt', 'gdelt_mentions') }}
    {% if is_incremental() %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= (select max(source_file_date) from {{ this }})
    {% else %}
        where date(split(metadata_filename, '/')[2], 'yyyymmdd') >= dateadd(week, -52, current_date)
    {% endif %}

),

format_fields as (

    select
        lower(cast(mention_identifier as string)) as mention_url,
        cast(gdelt_id as bigint) as gdelt_event_natural_key,

        source_file_date,
        to_date(left(event_time_date, 8), 'yyyymmdd') as event_time_date,
        to_date(left(mention_time_date, 8), 'yyyymmdd') as mention_time_date,
        lower(cast(mention_type as string)) as mention_type,
        lower(cast(mention_source_name as string)) as mention_source_name,
        cast(sentence_id as int) as sentence_id,
        cast(actor1_char_offset as int) as actor1_char_offset,
        cast(actor2_char_offset as int) as actor2_char_offset,
        cast(action_char_offset as int) as action_char_offset,
        cast(in_raw_text as int) as in_raw_text,
        cast(confidence as int) as confidence,
        lower(cast(mention_doc_len as string)) as mention_doc_len,
        lower(cast(mention_doc_tone as float)) as mention_doc_tone,
        lower(cast(mention_doc_translation_info as string)) as mention_doc_translation_info

    from source

),

{% set partition = '(partition by mention_url order by sentence_id, confidence desc)' %}

final as (

    select distinct
        mention_url,
        first_value(gdelt_event_natural_key) over {{ partition }} as gdelt_event_natural_key,

        first_value(source_file_date) over {{ partition }} as source_file_date,
        first_value(event_time_date) over {{ partition }} as event_time_date,
        first_value(mention_time_date) over {{ partition }} as mention_time_date,
        first_value(mention_type) over {{ partition }} as mention_type,
        first_value(mention_source_name) over {{ partition }} as mention_source_name,
        first_value(sentence_id) over {{ partition }} as sentence_id,
        first_value(actor1_char_offset) over {{ partition }} as actor1_char_offset,
        first_value(actor2_char_offset) over {{ partition }} as actor2_char_offset,
        first_value(action_char_offset) over {{ partition }} as action_char_offset,
        first_value(in_raw_text) over {{ partition }} as in_raw_text,
        first_value(confidence) over {{ partition }} as confidence,
        first_value(mention_doc_len) over {{ partition }} as mention_doc_len,
        first_value(mention_doc_tone) over {{ partition }} as mention_doc_tone,
        first_value(mention_doc_translation_info) over {{ partition }} as mention_doc_translation_info

    from format_fields

)


select * from final