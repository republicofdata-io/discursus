from dagster import op, AssetMaterialization, Output
from io import StringIO
import pandas as pd
from resources.ml_enrichment_tracker import MLEnrichmentJobTracker

@op(
    required_resource_keys = {"novacene_resource"},
    config_schema = {
        "asset_materialization_path": str
    }
)
def classify_mentions_relevancy(context): 
    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Get latest asset of gdelt articles
    filename = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1]
    obj = my_ml_enrichment_jobs_tracker.s3.Object('discursus-io', filename)
    df_gdelt_articles = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')))

    # Sending latest batch of articles to Novacene for relevancy classification
    if df_gdelt_articles.index.size > 0:
        context.log.info("Sending " + str(df_gdelt_articles.index.size) + " articles for relevancy classification")

        protest_classification_dataset_id = context.resources.novacene_resource.create_dataset("protest_events_" + filename.split("/")[3], df_gdelt_articles)
        protest_classification_job = context.resources.novacene_resource.enrich_dataset(protest_classification_dataset_id['id'], 17, 4)

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(protest_classification_job['id'], 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

        # Materialize asset
        yield AssetMaterialization(
            asset_key=["logs", "ml_enrichment_jobs"],
            description="List of ml enrichment jobs",
            metadata={
                "job id": protest_classification_job['id'],
                "dataset enriched": filename,
                "dataset enrtries": df_gdelt_articles.index.size
            }
        )
        yield Output(protest_classification_job)
    else:
        yield Output(1)