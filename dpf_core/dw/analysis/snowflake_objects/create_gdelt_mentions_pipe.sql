create pipe gdelt_events_pipe as
copy into gdelt_mentions
  from @s3_dio_sources/gdelt
  file_format = gdelt_csv
  pattern='.*.mentions.CSV'
  on_error = 'skip_file';