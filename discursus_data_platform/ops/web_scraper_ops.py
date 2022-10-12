from dagster import op, AssetMaterialization, Output

from io import StringIO
import pandas as pd

@op(
    required_resource_keys = {"web_scraper_resource"}
)
def scrape_urls(context, df_articles): 
    # Dedup articles
    df_articles = df_articles.drop_duplicates(subset=["5"], keep='first')

    # Create dataframe
    column_names = ['mention_identifier', 'file_name', 'title', 'description', 'keywords']
    df_scraped_urls = pd.DataFrame(columns = column_names)

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
        df_length = len(df_scraped_urls)
        df_scraped_urls.loc[df_length] = scraped_row

    return df_scraped_urls