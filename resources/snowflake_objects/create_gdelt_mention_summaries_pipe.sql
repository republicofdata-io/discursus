create pipe gdelt_mention_summaries_pipe as

copy into gdelt_mention_summaries(

  mention_identifier,
  summary,
  metadata_filename

)

from (

  select 
    t.$1,
    t.$2,
    metadata$filename

  from @s3_dio_sources/ml (

    file_format => csv,
    pattern => '.*mentions.summary.csv'

  ) t
);