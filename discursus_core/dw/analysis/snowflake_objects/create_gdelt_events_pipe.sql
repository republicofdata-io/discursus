create pipe gdelt_events_pipe as
copy into gdelt_events
  from @s3_dio_sources/gdelt
  file_format = gdelt_csv
  pattern='.*.export.CSV';