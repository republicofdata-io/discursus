cube (`dbt_metrics_default_calendar`, {
sql: `select * from ANALYTICS.dbt_metrics_default_calendar`,
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
sql: `select * from ANALYTICS.events_fct`,
dimensions: {
action_geo_country_code:  {
  sql: `action_geo_country_code`,
  type: `string`,
  description: `not available`,
} ,
action_geo_country_name:  {
  sql: `action_geo_country_name`,
  type: `string`,
  description: `not available`,
} ,
action_geo_full_name:  {
  sql: `action_geo_full_name`,
  type: `string`,
  description: `not available`,
} ,
event_date:  {
  sql: `event_date`,
  type: `time`,
  description: `not available`,
} ,
event_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `event_pk`,
  description: `not available`,
} ,
movement_fk:  {
  sql: `movement_fk`,
  type: `string`,
  description: `not available`,
} ,
}});
cube (`movements_dim`, {
sql: `select * from ANALYTICS.movements_dim`,
dimensions: {
countries:  {
  sql: `countries`,
  type: `string`,
  description: `not available`,
} ,
movement_name:  {
  sql: `movement_name`,
  type: `string`,
  description: `not available`,
} ,
movement_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `movement_pk`,
  description: `not available`,
} ,
page_description_regex:  {
  sql: `page_description_regex`,
  type: `string`,
  description: `not available`,
} ,
published_date_end:  {
  sql: `published_date_end`,
  type: `time`,
  description: `not available`,
} ,
published_date_start:  {
  sql: `published_date_start`,
  type: `time`,
  description: `not available`,
} ,
}});
cube (`observations_fct`, {
sql: `select * from ANALYTICS.observations_fct`,
dimensions: {
event_fk:  {
  sql: `event_fk`,
  type: `string`,
  description: `not available`,
} ,
observation_keywords:  {
  sql: `observation_keywords`,
  type: `string`,
  description: `not available`,
} ,
observation_page_description:  {
  sql: `observation_page_description`,
  type: `string`,
  description: `not available`,
} ,
observation_page_title:  {
  sql: `observation_page_title`,
  type: `string`,
  description: `not available`,
} ,
observation_pk:  {
  primaryKey: true,
  type: `string`,
  sql: `observation_pk`,
  description: `not available`,
} ,
observation_type:  {
  sql: `observation_type`,
  type: `string`,
  description: `not available`,
} ,
observation_url:  {
  sql: `observation_url`,
  type: `string`,
  description: `not available`,
} ,
published_date:  {
  sql: `published_date`,
  type: `time`,
  description: `not available`,
} ,
}});
