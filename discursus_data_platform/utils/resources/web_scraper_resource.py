from bs4 import BeautifulSoup
from dagster import resource
from http.client import IncompleteRead
import re
import socket
from urllib.request import Request, urlopen
from urllib.error import URLError
from urllib.parse import urlparse

class WebScraperResource:
    def __init__(self):
        # Compile regular expression to extract domain
        self.url_pattern = re.compile(r"www.(.+?)(.com|.net|.org)")


    def fetch_url_content(self, url):
        # Fetch the HTML content of the given URL using a custom user agent
        user_agent = 'Content-Audit/2.0'
        req = Request(url)
        req.add_header('User-Agent', user_agent)

        try:
            with urlopen(req, timeout=10) as response:
                return response.read()
        except URLError as e:
            print(f"Error fetching content for URL: {url}. Error: {e}")
            return None
        except socket.timeout:
            print(f"Timeout error for URL: {url}")
            return None
        except IncompleteRead as e:
            print(f"Error scraping article for URL: {url}: Error: {str(e)}")
        


    def parse_metadata(self, soup):
        # Extract metadata (title, description, keywords) from the parsed HTML soup
        try:
            title = str(soup.head.title.contents[0]) if soup.head.title else ''
        except AttributeError:
            title = ''

        try:
            description_tag = soup.find("meta", {"name": "description"})
            description = str(description_tag["content"]) if description_tag and "content" in description_tag.attrs else ''
        except (AttributeError, TypeError):
            description = ''

        try:
            keywords_tag = soup.find("meta", {"name": "keywords"})
            keywords = str(keywords_tag["content"]) if keywords_tag and "content" in keywords_tag.attrs else ''
        except (AttributeError, TypeError):
            keywords = ''

        return title, description, keywords




    def find_main_content(self, soup):
        # Find the main content of the article
        main_content = soup.find('main')
        if not main_content:
            main_content = soup.find('article')
        if not main_content:
            main_content = soup.find('div', class_=re.compile(r"(content|main-content|article-body)"))
        return main_content


    def extract_main_content(self, soup):
        main_content = self.find_main_content(soup)
        if main_content:
            return ' '.join([p.get_text(strip=True) for p in main_content.find_all('p')])
        else:
            return ''


    def scrape_article(self, url):
        # Scrape article metadata and content from the given URL
        html_content = self.fetch_url_content(url)
        if not html_content:
            return None

        soup = BeautifulSoup(html_content, "html.parser")
        title, description, keywords = self.parse_metadata(soup)

        # Extract the main content of the article
        main_content = self.extract_main_content(soup)

        article_data = {
            "url": url,
            "filename": urlparse(url).path,
            "title": title,
            "description": description,
            "keywords": keywords,
            "content": main_content
        }

        return article_data


@resource(description="A web scraper resource.")
def initiate_web_scraper_resource(context):
    return WebScraperResource()
