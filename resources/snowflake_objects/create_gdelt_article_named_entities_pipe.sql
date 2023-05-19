create pipe gdelt_article_named_entities_pipe as

copy into gdelt_article_named_entities(

  article_identifier,
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
    pattern => '.*articles.entities.csv'

  ) t
);