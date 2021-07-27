create pipe gdelt_enhanced_mentions_pipe as
copy into gdelt_enhanced_mentions
  from @s3_dio_sources/gdelt
  file_format = csv
  pattern='.*.mentions.enhanced.csv';