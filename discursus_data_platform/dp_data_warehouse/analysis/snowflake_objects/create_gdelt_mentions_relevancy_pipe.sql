create pipe gdelt_mentions_named_entities_pipe as

copy into gdelt_mentions_named_entities(

  mention_identifier,
  named_entities,
  metadata_filename

)

from (

  select 
    t.$3,
    t.$9,
    metadata$filename

  from @s3_dio_sources/ml (

    file_format => csv,
    pattern => '.*_entity_extraction.csv'

  ) t
);