create or replace table gdelt_mentions (
    
	gdelt_id number(38,0),
	event_time_date varchar(16777216),
	mention_time_date varchar(16777216),
	mention_type number(38,0),
	mention_source_name varchar(16777216),
	mention_identifier varchar(16777216),
	sentence_id number(38,0),
	actor1_char_offset number(38,0),
	actor2_char_offset number(38,0),
	action_char_offset number(38,0),
	in_raw_text number(38,0),
	confidence number(38,0),
	mention_doc_len number(38,0),
	mention_doc_tone number(38,0),
	mention_doc_translation_info varchar(16777216),
	extras varchar(16777216),
    metadata_filename varchar

)