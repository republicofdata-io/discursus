create pipe gdelt_mention_named_entities_pipe as

copy into gdelt_mention_named_entities(

  mention_identifier,
  named_entities,
  metadata_filename

)

from (

  select 
    t.$1,
    t.$2,
    metadata$filename

  from @s3_dio_sources/ml (

    file_format => csv,
    pattern => '.*mentions.entities.csv'

  ) t
);