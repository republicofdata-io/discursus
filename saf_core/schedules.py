from dagster import ScheduleDefinition
from jobs import my_job

my_job_schedule = ScheduleDefinition(job = my_job, cron_schedule = "2,17,32,47 * * * *")