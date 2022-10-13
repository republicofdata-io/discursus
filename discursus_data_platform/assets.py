from dagster import asset, Output
import pandas as pd
import resources.my_resources


@asset(
    description = "List of events mined on GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_events(context):
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    
    # Build source path
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    latest_events_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', latest_events_source_path)

    # Return asset
    return Output(
        df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_events_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )


@asset(
    non_argument_deps = {"gdelt_events"},
    description = "List of mentions mined from GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource
    }
)
def gdelt_mentions(context):
    file_path = context.op_config["file_path"].split("s3://discursus-io/")[1]

    df_latest_events_filtered = context.resources.aws_resource.s3_get('discursus-io', file_path)
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")

    # Build source path
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    latest_mentions_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', latest_mentions_source_path)

    # Return asset
    return Output(
        df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + latest_mentions_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )


@asset(
    non_argument_deps = {"gdelt_mentions"},
    description = "List of enhanced mentions mined from GDELT",
    group_name = "sources",
    resource_defs = {
        'aws_resource': resources.my_resources.my_aws_resource,
        'gdelt_resource': resources.my_resources.my_gdelt_resource,
        'web_scraper_resource': resources.my_resources.my_web_scraper_resource
    }
)
def gdelt_enhanced_mentions(context):
    file_path = context.op_config["file_path"].split("s3://discursus-io/")[1]
    enhanced_mentions_source_path = context.op_config["asset_materialization_path"].split("s3://discursus-io/")[1].split(".CSV")[0] + ".enhanced.csv"
    df_latest_mentions_filtered = context.resources.aws_resource.s3_get('discursus-io', file_path)

    # Dedup articles
    df_articles = df_latest_mentions_filtered.drop_duplicates(subset=["5"], keep='first')

    # Create dataframe
    column_names = ['mention_identifier', 'file_name', 'title', 'description', 'keywords']
    df_gdelt_enhanced_mentions = pd.DataFrame(columns = column_names)

    # Scrape urls and populate dataframe
    for index, row in df_articles.iterrows():
        scraped_article = context.resources.web_scraper_resource.scrape_url(row[5])
        scraped_row = [
            scraped_article['mention_identifier'][0], 
            scraped_article['file_name'][0],
            scraped_article['title'][0],
            scraped_article['description'][0],
            scraped_article['keywords'][0]
        ]
        df_length = len(df_gdelt_enhanced_mentions)
        df_gdelt_enhanced_mentions.loc[df_length] = scraped_row
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_enhanced_mentions, 'discursus-io', enhanced_mentions_source_path)

    # Return asset
    return Output(
        df_gdelt_enhanced_mentions, 
        metadata = {
            "path": "s3://discursus-io/" + enhanced_mentions_source_path,
            "rows": df_gdelt_enhanced_mentions.index.size
        }
    )