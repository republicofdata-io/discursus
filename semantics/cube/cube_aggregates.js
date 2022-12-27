cube (`dbt_metrics_default_calendar_extended`, {
sql: `select * from ANALYTICS.dbt_metrics_default_calendar`,
extends: dbt_metrics_default_calendar,
measures: {
}});
cube (`events_fct_extended`, {
sql: `select * from ANALYTICS.events_fct`,
extends: events_fct,
measures: {
sum_of_action_geo_latitude:  {
  sql: `action_geo_latitude`,
  type: `sum`,
} ,
sum_of_action_geo_longitude:  {
  sql: `action_geo_longitude`,
  type: `sum`,
} ,
count_of_event_pk:  {
  sql: `event_pk`,
  type: `count`,
} ,
}});
cube (`movements_dim_extended`, {
sql: `select * from ANALYTICS.movements_dim`,
extends: movements_dim,
measures: {
count_of_movement_pk:  {
  sql: `movement_pk`,
  type: `count`,
} ,
}});
cube (`observations_fct_extended`, {
sql: `select * from ANALYTICS.observations_fct`,
extends: observations_fct,
measures: {
count_of_observation_pk:  {
  sql: `observation_pk`,
  type: `count`,
} ,
}});
