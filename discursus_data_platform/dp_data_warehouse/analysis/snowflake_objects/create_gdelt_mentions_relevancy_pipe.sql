create pipe gdelt_mentions_relevancy_pipe as

copy into gdelt_mentions_relevancy(

  mention_identifier,
  page_name,
  file_name,
  page_title,
  page_description,
  keywords,
  metadata_filename,
  is_relevant

)

from (

  select 
    t.$2,
    t.$3,
    t.$3,
    t.$4,
    t.$5,
    t.$6,
    metadata$filename,
    t.$7

  from @s3_dio_sources/ml (

    file_format => csv,
    pattern => '.*.mentions.enhanced.csv'

  ) t
);