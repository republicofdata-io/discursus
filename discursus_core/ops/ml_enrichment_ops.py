from dagster import op, AssetMaterialization, Output

@op(
    required_resource_keys={"novacene_client"}
)
def get_protest_relevancy(context):
    my_datasets = context.resources.novacene_client.list_datasets()
    context.log.info(my_datasets)
    #yield AssetMaterialization(asset_key="novacene_jobs", description="List of jobs being ran by Novacene")
    #yield Output(job_list)