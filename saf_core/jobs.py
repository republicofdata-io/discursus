from dagster import job
from ops.my_ops import my_op


@job
def my_job():
    my_op_result = my_op()