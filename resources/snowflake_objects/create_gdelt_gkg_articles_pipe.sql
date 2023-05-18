create pipe gdelt_gkg_articles_pipe as

copy into gdelt_gkg_articles(

  gdelt_gkg_id,
  gdelt_gkg_date_time,
  source_collection_id,
  source_domain,
  article_identifier,
  counts,
  themes,
  locations,
  persons,
  organizations,
  article_image_url,
  article_video_url,
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
    metadata$filename::string

  from @s3_dio_sources/gdelt (

    file_format => csv,
    pattern => '.*.gkg.csv'

  ) t
);