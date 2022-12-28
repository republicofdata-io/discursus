cube (`dbt_metrics_default_calendar`, {
sql: `select * from ANALYTICS_QA.dbt_metrics_default_calendar`,
dimensions: {
date_day:  {
  sql: `date_day`,
  type: `time`,
  description: `not available`,
} ,
date_month:  {
  sql: `date_month`,
  type: `time`,
  description: `not available`,
} ,
date_quarter:  {
  sql: `date_quarter`,
  type: `time`,
  description: `not available`,
} ,
date_week:  {
  sql: `date_week`,
  type: `time`,
  description: `not available`,
} ,
date_year:  {
  sql: `date_year`,
  type: `time`,
  description: `not available`,
} ,
}});
cube (`events_fct`, {
sql: `select * from ANALYTICS_QA.events_fct`,
dimensions: {
action_geo_country_code:  {
  sql: `action_geo_country_code`,
  type: `string`,
  description: `action geo country codes (agcc) are codes assigned to countries of the world by the action geo data company. the codes consist of five numbers and are used to classify countries by geographical region. action geo country codes are used in data analysis related to geospatial applications, including mapping, marketing, and population studies.`,
} ,
action_geo_country_name:  {
  sql: `action_geo_country_name`,
  type: `string`,
  description: `action geo country name is a geography quiz game developed by actioncandy. it is a fun, interactive and educational game designed to test a player’s knowledge of various countries, cities and world flags. by answering questions such as “what country is the eiffel tower located in?” or “what is the capital of canada?”, players are able to unlock rewards, complete challenges and build up their geography knowledge.`,
} ,
action_geo_full_name:  {
  sql: `action_geo_full_name`,
  type: `string`,
  description: `actiongeo is an abbreviation for action geographic education outreach, an educational outreach program founded in 2008 by the national geographic society to address the need for geography education in the classroom.`,
} ,
event_date:  {
  sql: `event_date`,
  type: `time`,
  description: `event date is the day or days that a particular event is scheduled to take place.`,
} ,
event_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `event_pk`,
  description: `an event primary key is a unique identifier associated with an event, such as a conference, corporate gathering, or other special event. the primary key is typically assigned to each attendee and is used to help control event access and track attendance.`,
} ,
movement_fk:  {
  sql: `movement_fk`,
  type: `string`,
  description: `a foreign key is a column or group of columns in a relational database table that provides a link between data in two tables. it acts as a cross-reference between tables because it references theprimary key of another table, thereby establishing a link between them. a foreign key can be thought of as “pointing” to its respective primary key in another table.`,
} ,
}});
cube (`movements_dim`, {
sql: `select * from ANALYTICS_QA.movements_dim`,
dimensions: {
countries:  {
  sql: `countries`,
  type: `string`,
  description: `countries are political entities made up of a certain number of people living in a certain geographic area and subject to a common government. they are generally recognized as sovereign states, though some countries may also be autonomous regions or other supranational entities.`,
} ,
movement_name:  {
  sql: `movement_name`,
  type: `string`,
  description: `movement names vary widely depending on the type of movement being discussed. examples of different types of movement include the dada movement, the impressionist movement, the harlem renaissance, the civil rights movement, the women's movement, and even the occupy movement.`,
} ,
movement_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `movement_pk`,
  description: `movement primary key is a unique identifier assigned to each record in a database table that identifies records in that table and helps to provide data integrity. it is used to ensure that records in the database are unique and are not duplicated.`,
} ,
page_description_regex:  {
  sql: `page_description_regex`,
  type: `string`,
  description: `page description regex is a regular expression used to match against webpage content and extract the page description text. it is typically used by search engines to index the contents of a webpage.`,
} ,
published_date_end:  {
  sql: `published_date_end`,
  type: `time`,
  description: `there is no definitive answer to this question as it depends on the type of publication. if you are referring to a specific publication, contact the publisher directly to find out the publication date end.`,
} ,
published_date_start:  {
  sql: `published_date_start`,
  type: `time`,
  description: `there is no universal definition for the term \"published date start\". generally speaking, the published date of a book, article, or other work is the date that it was first released, or made available to the public. this could be on a specific date, or it could be available starting from a certain date (e.g., a book might be released on march 1st or available from march 1st onward).`,
} ,
}});
cube (`observations_fct`, {
sql: `select * from ANALYTICS_QA.observations_fct`,
dimensions: {
event_fk:  {
  sql: `event_fk`,
  type: `string`,
  description: `an event foreign key is a reference to a primary key in another table. it is used to link two tables together and identify related records in each table. for example, a company's employees table may contain employee records and an events table may contain information about events that employees attend. the foreign key in the events table would then point to the employees table, allowing the two tables to be linked together.`,
} ,
observation_keywords:  {
  sql: `observation_keywords`,
  type: `string`,
  description: `observation keywords are adjectives and descriptive phrases used to describe observed behaviors, attitudes, and characteristics. they are used to document and analyze behavior in a systematic way, allowing for the comparison and analysis of large amounts of data.`,
} ,
observation_page_description:  {
  sql: `observation_page_description`,
  type: `string`,
  description: `observation page description is a brief description used to summarise the key information found in an observation page. it should include any relevant facts, observations and data that was noted during the observations, as well as any conclusions that were drawn. it should also provide an overall summary of the observations, providing readers with a clear understanding of the subject matter.`,
} ,
observation_page_title:  {
  sql: `observation_page_title`,
  type: `string`,
  description: `the observation page title is an indication of what the page is about. it should be a concise and descriptive headline that accurately reflects the information contained on the page.`,
} ,
observation_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `observation_pk`,
  description: `the primary key in an observation table is the unique identifier of individual observations in a dataset. usually, a primary key is composed of one or more columns that contain a value for each row of data to ensure that each row can be uniquely identified. examples of primary keys for observation data may include an id column or a combination of name, gender, and age columns.`,
} ,
observation_type:  {
  sql: `observation_type`,
  type: `string`,
  description: `observation type is a method of data collection that involves directly observing variables of interest in their natural environment without manipulating them in any way. observational types include observational studies, surveys, questionnaires, semi-structured interviews, archival data, and naturalistic observations.`,
} ,
observation_url:  {
  sql: `observation_url`,
  type: `string`,
  description: `observation url is a url used to access web-based observations made by satellites, weather radar, and other remote sensing devices. the url typically references an online mapping service that displays the geographically referenced observation data over an interactive map.`,
} ,
published_date:  {
  sql: `published_date`,
  type: `time`,
  description: `the published date is the date when a piece of content or material was first released to the public. it is usually the date that is displayed on the cover or title page of a book, article, or other document or media.`,
} ,
}});
