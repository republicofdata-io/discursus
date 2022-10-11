from dagster import repository
from jobs import my_job
from schedules import my_job_schedule

@repository
def saf_repository():
    jobs = [
        my_job
    ]
    schedules = [
        my_job_schedule
    ]

    return jobs + schedules