create pipe gdelt_mentions_pipe as

copy into gdelt_mentions(

    gdelt_id,
    event_time_date,
    mention_time_date,
    mention_type,
    mention_source_name,
    mention_identifier,
    sentence_id,
    actor1_char_offset,
    actor2_char_offset,
    action_char_offset,
    in_raw_text,
    confidence,
    mention_doc_len,
    mention_doc_tone,
    mention_doc_translation_info,
    extras,
    metadata_filename

)

from (

  select 
    t.$1,
    t.$2,
    t.$3,
    t.$4,
    t.$5,
    t.$6,
    t.$7,
    t.$8,
    t.$9,
    t.$10,
    t.$11,
    t.$12,
    t.$13,
    t.$14,
    t.$15,
    t.$16,
    metadata$filename

  from @s3_dio_sources/gdelt (

    file_format => csv,
    pattern => '.*.mentions.CSV'

  ) t
);