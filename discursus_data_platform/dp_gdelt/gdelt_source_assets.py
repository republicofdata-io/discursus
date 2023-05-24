from dagster import asset, AssetIn, Output, FreshnessPolicy, AutoMaterializePolicy
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
import pandas as pd
import boto3
from io import StringIO
from urllib.request import urlretrieve
import zipfile

from discursus_data_platform.utils.resources import my_resources


@asset(
    description = "List of events mined on GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy = FreshnessPolicy(maximum_lag_minutes=15),
)
def gdelt_events(context):
    # Build source path
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("events")
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_events = context.resources.gdelt_resource.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = context.resources.gdelt_resource.filter_latest_events(df_latest_events, 14, ['US', 'CA'])
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_events_filtered, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_events = "alter pipe gdelt_events_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_events)

    # Return asset
    return Output(
        value = df_latest_events_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_events_filtered.index.size
        }
    )


@asset(
    ins = {"gdelt_events": AssetIn(key_prefix = "gdelt")},
    description = "List of mentions mined from GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_mentions(context, gdelt_events):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mine and filter data
    df_latest_mentions = context.resources.gdelt_resource.mine_latest_asset(latest_mentions_url)
    df_latest_mentions_filtered = context.resources.gdelt_resource.filter_latest_mentions(df_latest_mentions, gdelt_events)
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_mentions_filtered, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mentions_events = "alter pipe gdelt_mentions_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_events)

    # Return asset
    return Output(
        value = df_latest_mentions_filtered, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_mentions_filtered.index.size
        }
    )


@asset(
    description = "List of gkg articles mined on GDELT",
    key_prefix = ["gdelt"],
    group_name = "sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.lazy(),
)
def gdelt_gkg_articles(context):
    # Build source path
    latest_events_url = context.resources.gdelt_resource.get_url_to_latest_asset("gkg")
    gdelt_asset_filename_zip = str(latest_events_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    # Mines the latest asset from GDELT    
    urlretrieve(latest_events_url, gdelt_asset_filename_zip)
    with zipfile.ZipFile(gdelt_asset_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')
    df_latest_gkg_articles = pd.read_csv(gdelt_asset_filename_csv, sep='\t', header=None, encoding='ISO-8859-1')

    # Filter data
    df_latest_gkg_articles = df_latest_gkg_articles.iloc[:, [0, 1, 2, 3, 4, 5, 7, 9, 11, 13, 18, 21]]

    # Rename the columns for better readability
    df_latest_gkg_articles.columns = [
        "gdelt_gkg_id",
        "gdelt_gkg_date_time",
        "source_collection_id",
        "source_domain",
        "article_identifier",
        "counts",
        "themes",
        "locations",
        "persons",
        "organizations",
        "article_image_url",
        "article_video_url",
    ]

    # Convert specific columns to lowercase
    df_latest_gkg_articles[
        ["counts", "themes", "locations", "persons", "organizations"]
    ] = df_latest_gkg_articles[
        ["counts", "themes", "locations", "persons", "organizations"]
    ].applymap(
        lambda x: x.lower() if isinstance(x, str) else x
    )

    # Filter the DataFrame to only include rows with source_collection_id equal to 1
    df_latest_gkg_articles_filtered = df_latest_gkg_articles[
        df_latest_gkg_articles["source_collection_id"] == 1
    ]

    # Filter the DataFrame to only include rows with the "locations" column containing the words "united states" or "canada"
    # Split the locations using the ; delimiter and search for the word "united states" or "canada" in the the first element of the list
    df_latest_gkg_articles_filtered = df_latest_gkg_articles_filtered[
        (df_latest_gkg_articles["locations"].str.split(";", expand=True)[0].str.contains("united states", na=False))
        | (df_latest_gkg_articles["locations"].str.split(";", expand=True)[0].str.contains("canada", na=False))
    ]

    # Split the themes using the ; delimiter and search for the word "protest" in the the first 10 elements of the list
    df_latest_gkg_articles_filtered = df_latest_gkg_articles_filtered[
        (df_latest_gkg_articles["themes"].str.split(";", expand=True)[0].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[1].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[2].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[3].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[4].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[5].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[6].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[7].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[8].str.contains("protest", na=False))
        | (df_latest_gkg_articles["themes"].str.split(";", expand=True)[9].str.contains("protest", na=False))
    ]

    # Deduplicate the DataFrame
    df_latest_gkg_articles_filtered = df_latest_gkg_articles_filtered.drop_duplicates()
    
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_latest_gkg_articles_filtered, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_gkg_articles = "alter pipe gdelt_gkg_articles_pipe refresh;"
    context.resources.snowflake_resource.execute_query(q_load_gdelt_gkg_articles)

    # Return asset
    return Output(
        value = df_latest_gkg_articles_filtered, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_latest_gkg_articles_filtered.index.size
        }
    )
