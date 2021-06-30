create table gdelt_mentions (

  gdelt_id int, 
  event_time_date varchar, 
  mention_time_date varchar, 
  mention_type int,
  mention_source_name varchar, 
  mention_identifier varchar, 
  sentence_id int, 
  actor1_char_offset int, 
  actor2_char_offset int, 
  action_char_offset int, 
  in_raw_text int, 
  confidence int, 
  mention_doc_len int, 
  mention_doc_tone int, 
  mention_doc_translation_info varchar, 
  extras varchar

)