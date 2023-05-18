create or replace table gdelt_gkg_articles (
    
	gdelt_gkg_id varchar(16777216),
	gdelt_gkg_date_time varchar(16777216),
	source_collection_id number(38,0),
	source_domain varchar(16777216),
	article_identifier varchar(16777216),
	counts varchar(16777216),
	themes varchar(16777216),
	locations varchar(16777216),
	persons varchar(16777216),
	organizations varchar(16777216),
	article_image_url varchar(16777216),
	article_video_url varchar(16777216),
    metadata_filename varchar

)