create pipe gdelt_article_summaries_pipe as

copy into gdelt_article_summaries(

  article_identifier,
  summary,
  metadata_filename

)

from (

  select 
    t.$1,
    t.$2,
    metadata$filename

  from @s3_dio_sources/gdelt (

    file_format => csv,
    pattern => '.*articles.summary.csv'

  ) t
);