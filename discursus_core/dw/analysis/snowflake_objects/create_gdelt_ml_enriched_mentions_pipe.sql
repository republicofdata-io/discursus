create pipe gdelt_ml_enriched_mentions_pipe as

copy into gdelt_ml_enriched_mentions(

  mention_identifier,
  page_name,
  file_name,
  page_title,
  page_description,
  keywords,
  is_relevant,
  metadata_filename

)

from (

  select 
    t.$2,
    t.$3,
    t.$4,
    t.$5,
    t.$6,
    t.$7,
    t.$8,
    metadata$filename

  from @s3_dio_sources/ml (

    file_format => csv,
    pattern => '.*.export.enhanced.csv'

  ) t
);