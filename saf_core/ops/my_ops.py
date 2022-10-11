from dagster import op

@op
def my_op(context):
    my_op_result = 'Hello World!'
    context.log.info(str(my_op_result))

    return my_op_result