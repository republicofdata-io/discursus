from dagster import asset, AssetKey, AssetIn, AssetObservation, Output, FreshnessPolicy, AutoMaterializePolicy
import pandas as pd
import spacy
import time
import openai.error

from discursus_data_platform.utils.resources import my_resources


@asset(
    ins = {"gdelt_mentions": AssetIn(key_prefix = "gdelt")},
    description = "List of enhanced mentions mined from GDELT",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'web_scraper_resource': my_resources.my_web_scraper_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_mentions_enhanced(context, gdelt_mentions):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.mentions.enhanced.csv'

    # Dedup articles
    df_articles = gdelt_mentions.drop_duplicates(subset=[5], keep='first')

    # Create dataframe
    column_names = ['mention_identifier', 'file_name', 'title', 'description', 'keywords', 'content']
    df_gdelt_mentions_enhanced = pd.DataFrame(columns = column_names)

    for _, row in df_articles.iterrows():
        scraped_article = context.resources.web_scraper_resource.scrape_article(row[5])

        if scraped_article is None:
            scraped_article = {}
    
        # Use get method with default value (empty string) for each element in scraped_row
        scraped_row = [
            scraped_article.get('url', ''),
            scraped_article.get('filename', ''),
            scraped_article.get('title', ''),
            scraped_article.get('description', ''),
            scraped_article.get('keywords', ''),
            scraped_article.get('content', '')
        ]
        
        df_length = len(df_gdelt_mentions_enhanced)
        df_gdelt_mentions_enhanced.loc[df_length] = scraped_row # type: ignore
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_mentions_enhanced, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mentions_enhanced_events = "alter pipe gdelt_enhanced_mentions_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mentions_enhanced_events)

    # Return asset
    return Output(
        value = df_gdelt_mentions_enhanced, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_mentions_enhanced.index.size
        }
    )


@asset(
    ins = {"gdelt_gkg_articles": AssetIn(key_prefix = "gdelt")},
    description = "List of enhanced articles mined from GDELT",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'web_scraper_resource': my_resources.my_web_scraper_resource,
        'snowflake_resource': my_resources.my_snowflake_resource,
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_enhanced_articles(context, gdelt_gkg_articles):
    # Build source path
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.articles.enhanced.csv'

    # Dedup articles
    df_articles = gdelt_gkg_articles.drop_duplicates(subset=['article_identifier'], keep='first')

    # Create dataframe
    column_names = ['mention_identifier', 'file_name', 'title', 'description', 'keywords', 'content']
    df_gdelt_enhanced_articles = pd.DataFrame(columns = column_names)

    for _, row in df_articles.iterrows():
        scraped_article = context.resources.web_scraper_resource.scrape_article(row['article_identifier'])

        if scraped_article is None:
            scraped_article = {}
    
        # Use get method with default value (empty string) for each element in scraped_row
        scraped_row = [
            scraped_article.get('url', ''),
            scraped_article.get('filename', ''),
            scraped_article.get('title', ''),
            scraped_article.get('description', ''),
            scraped_article.get('keywords', ''),
            scraped_article.get('content', '')
        ]
        
        df_length = len(df_gdelt_enhanced_articles)
        df_gdelt_enhanced_articles.loc[df_length] = scraped_row # type: ignore
    
    # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_enhanced_articles, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_enhanced_articles = "alter pipe gdelt_enhanced_articles_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_enhanced_articles)

    # Return asset
    return Output(
        value = df_gdelt_enhanced_articles, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_enhanced_articles.index.size
        }
    )


@asset(
    ins = {"gdelt_mentions_enhanced": AssetIn(key_prefix = "gdelt")},
    description = "LLM-generated summary of GDELT mention",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'openai_resource': my_resources.my_openai_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_mention_summaries(context, gdelt_mentions_enhanced):
    # Build sourcepath
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/ml/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.mentions.summary.csv'

    # Cycle through each article in gdelt_mentions_enhanced and generate a summary
    df_gdelt_mention_summaries = pd.DataFrame(columns = ['mention_identifier', 'summary'])
    for _, row in gdelt_mentions_enhanced.iterrows():
        prompt = f"""Write a concise summary for the following article:
        
        Title: {row['title']}
        Description: {row['description']}
        Content: {row['content'][:3000]}

        CONCISE SUMMARY:"""
    
        # Keep retrying the request until it succeeds
        while True:
            try:
                completion_str = context.resources.openai_resource.chat_completion(model='gpt-3.5-turbo', prompt=prompt, max_tokens=2048)
                break
            except openai.error.RateLimitError as e:
                # Wait for 5 seconds before retrying
                time.sleep(5)
                continue

        df_length = len(df_gdelt_mention_summaries)
        df_gdelt_mention_summaries.loc[df_length] = [row['mention_identifier'], completion_str] # type: ignore

     # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_mention_summaries, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mention_summaries_events = "alter pipe gdelt_mention_summaries_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mention_summaries_events)

    # Return asset
    return Output(
        value = df_gdelt_mention_summaries, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_mention_summaries.index.size
        }
    )


@asset(
    ins = {"gdelt_enhanced_articles": AssetIn(key_prefix = "gdelt")},
    description = "LLM-generated summary of GDELT articles",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'openai_resource': my_resources.my_openai_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_article_summaries(context, gdelt_enhanced_articles):
    # Build sourcepath
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/ml/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.articles.summary.csv'

    # Cycle through each article in gdelt_mentions_enhanced and generate a summary
    df_gdelt_article_summaries = pd.DataFrame(columns = ['mention_identifier', 'summary'])
    for _, row in gdelt_enhanced_articles.iterrows():
        prompt = f"""Write a concise summary for the following article:
        
        Title: {row['title']}
        Description: {row['description']}
        Content: {row['content'][:3000]}

        CONCISE SUMMARY:"""
    
        # Keep retrying the request until it succeeds
        while True:
            try:
                completion_str = context.resources.openai_resource.chat_completion(model='gpt-3.5-turbo', prompt=prompt, max_tokens=2048)
                break
            except openai.error.RateLimitError as e:
                # Wait for 5 seconds before retrying
                time.sleep(5)
                continue

        df_length = len(df_gdelt_article_summaries)
        df_gdelt_article_summaries.loc[df_length] = [row['mention_identifier'], completion_str] # type: ignore

     # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_article_summaries, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_article_summaries_events = "alter pipe gdelt_article_summaries_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_article_summaries_events)

    # Return asset
    return Output(
        value = df_gdelt_article_summaries, 
        metadata = {
            "s3_path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_article_summaries.index.size
        }
    )


@asset(
    ins = {"gdelt_mention_summaries": AssetIn(key_prefix = "gdelt")},
    description = "Entity extraction of GDELT mention",
    key_prefix = ["gdelt"],
    group_name = "prepared_sources",
    resource_defs = {
        'aws_resource': my_resources.my_aws_resource,
        'gdelt_resource': my_resources.my_gdelt_resource,
        'snowflake_resource': my_resources.my_snowflake_resource
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def gdelt_mention_entity_extraction(context, gdelt_mention_summaries):
    # Build sourcepath
    latest_mentions_url = context.resources.gdelt_resource.get_url_to_latest_asset("mentions")
    gdelt_asset_filename_zip = str(latest_mentions_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_source_path = 'sources/ml/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv[0:14] + '.mentions.entities.csv'

    # Cycle through each article in gdelt_mention_summaries and extract entities
    df_gdelt_mention_entities = pd.DataFrame(columns = ['mention_identifier', 'named_entities'])
    nlp = spacy.load("en_core_web_sm")

    for _, row in gdelt_mention_summaries.iterrows():
        entities = [ent.text for ent in nlp(row["summary"]).ents if ent.label_ in ["PERSON", "ORG"]]

        df_length = len(df_gdelt_mention_entities)
        df_gdelt_mention_entities.loc[df_length] = [row['mention_identifier'], list(set(entities))] # type: ignore

     # Save data to S3
    context.resources.aws_resource.s3_put(df_gdelt_mention_entities, 'discursus-io', gdelt_asset_source_path)

    # Transfer to Snowflake
    q_load_gdelt_mention_named_entities_events = "alter pipe gdelt_mention_named_entities_pipe refresh;"
    snowpipe_result = context.resources.snowflake_resource.execute_query(q_load_gdelt_mention_named_entities_events)

    # Return asset
    return Output(
        value = df_gdelt_mention_entities, 
        metadata = {
            "path": "s3://discursus-io/" + gdelt_asset_source_path,
            "rows": df_gdelt_mention_entities.index.size
        }
    )