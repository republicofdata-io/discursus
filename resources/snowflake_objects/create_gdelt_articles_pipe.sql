create pipe gdelt_articles_pipe as

copy into gdelt_articles(

  gdelt_gkg_article_id,
  article_url,
  source_collection_id,
  themes,
  locations,
  primary_location,
  persons,
  organizations,
  social_image_url,
  social_video_url,
  creation_ts,
  dagster_partition_id,
  bq_partition_id,
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
    metadata$filename::string

  from @s3_dio_sources/gdelt (

    file_format => csv,
    pattern => '.*.articles.csv'

  ) t
);