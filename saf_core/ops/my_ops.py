from dagster import op

@op
def my_op(context):
    my_op_result = 'Hello World!'

    return my_op_result