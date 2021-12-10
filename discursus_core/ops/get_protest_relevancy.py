from dagster import op

@op(
    required_resource_keys={"novacene_client"}
)
def get_protest_relevancy(context):
    context.resources.novacene_client.list_jobs
    context.log.info("Hello Novacene!")