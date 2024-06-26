create pipe gdelt_enhanced_articles_pipe as

copy into gdelt_enhanced_articles(

  article_url,
  file_name,
  page_title,
  page_description,
  keywords,
  metadata_filename

)

from (

  select 
    t.$1,
    t.$2,
    t.$3,
    t.$4,
    t.$5,
    metadata$filename

  from @s3_dio_sources/gdelt (

    file_format => csv,
    pattern => '.*.articles.enhanced.csv'

  ) t
);