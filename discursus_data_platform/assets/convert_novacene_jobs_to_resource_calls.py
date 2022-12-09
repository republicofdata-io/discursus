from dagster import asset, AssetObservation, Output
import pandas as pd
import boto3
from io import StringIO
import resources.my_resources
from resources.ml_enrichment_tracker import MLEnrichmentJobTracker



@asset(
    description = "Jobs to classify relevancy of GDELT mentions",
    group_name = "prepared_sources",
    resource_defs = {
        'gdelt_resource': resources.my_resources.my_gdelt_resource,
        'novacene_resource': resources.my_resources.my_novacene_resource
    }
)
def gdelt_mentions_relevancy_ml_jobs(context, gdelt_mentions_enhanced): 
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Create instance of ml enrichment tracker
    my_ml_enrichment_jobs_tracker = MLEnrichmentJobTracker()
    
    # Sending latest batch of articles to Novacene for relevancy classification
    if gdelt_mentions_enhanced.index.size > 0:
        context.log.info("Sending " + str(gdelt_mentions_enhanced.index.size) + " articles for relevancy classification")

        protest_classification_dataset_id = context.resources.novacene_resource.create_dataset("protest_events_" + gdelt_asset_source_path.split("/")[3], gdelt_mentions_enhanced)
        protest_classification_job = context.resources.novacene_resource.enrich_dataset(protest_classification_dataset_id['id'], 17, 4)

        # Update log of enrichment jobs
        my_ml_enrichment_jobs_tracker.add_new_job(protest_classification_job['id'], 'processing')
        my_ml_enrichment_jobs_tracker.upload_job_log()

        # Return asset
        return Output(
            value = protest_classification_job, 
            metadata = {
                "job id": protest_classification_job['id'],
                "dataset enriched": gdelt_asset_source_path,
                "dataset enrtries": gdelt_mentions_enhanced.index.size
            }
        )
    else:
        return Output(1)