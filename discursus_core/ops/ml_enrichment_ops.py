from dagster import op, AssetMaterialization, Output

import boto3
from io import StringIO
import pandas as pd

@op(
    required_resource_keys = {"novacene_client"},
    config_schema = {
        "asset_key": list,
        "asset_materialization_path": str
    }
)
def classify_protest_relevancy(context): 
    # Get latest asset of gdelt articles
    filename = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1]
    context.log.info(filename)
    s3 = boto3.resource('s3')
    obj = s3.Object('discursus-io', filename)
    df_gdelt_articles = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

    # Sending latest batch of articles to Novacene for relevancy classification
    context.log.info("Sending " + str(df_gdelt_articles.index.size) + " articles for relevancy classification")

    #my_datasets = context.resources.novacene_client.list_datasets()
    #context.log.info(my_datasets)
    #yield AssetMaterialization(asset_key="novacene_jobs", description="List of jobs being ran by Novacene")
    #yield Output(job_list)