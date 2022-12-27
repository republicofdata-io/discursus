cube (`events_fct`, {
sql: `select * from ANALYTICS.events_fct`,
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
