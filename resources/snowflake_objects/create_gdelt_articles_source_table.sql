create or replace table gdelt_articles (
    
	gdelt_gkg_article_id varchar(16777216),
	article_url varchar(16777216),
	source_collection_id number(38,0),
	themes varchar(16777216),
	locations varchar(16777216),
	primary_location varchar(16777216),
	persons varchar(16777216),
	organizations varchar(16777216),
	social_image_url varchar(16777216),
	social_video_url varchar(16777216),
	creation_ts timestamp,
	dagster_partition_id number(38,0),
	bq_partition_id timestamp,
    metadata_filename varchar

)