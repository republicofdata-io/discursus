create pipe gdelt_events_pipe as
copy into gdelt_events
  from @s3_dio_sources/gdelt
  file_format = csv
  pattern='.*.export.CSV'
  on_error = 'skip_file';