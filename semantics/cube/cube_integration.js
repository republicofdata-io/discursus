cube (`events_fct`, {
sql: `select * from ANALYTICS_QA.events_fct`,
joins : {
observations_fct:  {
  relationship: `hasMany`,
  sql: `${CUBE.event_pk} = ${observations_fct.event_fk}`,
} ,
},
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
joins : {
events_fct:  {
  relationship: `hasMany`,
  sql: `${CUBE.movement_pk} = ${events_fct.movement_fk}`,
} ,
},
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
