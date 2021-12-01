# Credit goes to...
# https://github.com/quakerpunk/content-audit/blob/origin/content_audit.py

from dagster import op
from bs4 import BeautifulSoup
from optparse import OptionParser
import urllib.request
import urllib.error
import random
import csv
import urllib.parse
import re
import time
import boto3
import os

class ContentAuditor:

    def __init__(self, filename):
        """
        Initialization method for the ContentAuditor class.
        """
        s3 = boto3.resource('s3')
        obj = s3.Object('discursus-io', filename)

        self.filehandle = obj.get()['Body'].read().decode('utf-8')
        self.article_urls = []
        self.soupy_data = ""
        self.csv_output = ""
        self.content = ""
        self.text_file = ""
        self.site_info = []
        self.url_parts = ""
        self.reg_expres = re.compile(r"www.(.+?)(.com|.net|.org)")
    

    def get_list_of_urls(self):
        """
        Method which iterates over list of articles, only keep relevant ones and deduplicates.
        """
        for line in self.filehandle.splitlines():
            line_url = line.split("\t")[60].strip()
            if int(line.split("\t")[28]) == 14:
                continue
            if line.split("\t")[12].strip() != '':
                continue
            if line.split("\t")[12].strip() != '':
                continue
            if line.split("\t")[12].strip() != line.split("\t")[12].strip():
                continue

            if line_url != '': 
                self.article_urls.append(line_url)
            self.article_urls = list(set(self.article_urls))


    def read_url(self):
        """
        Method which reads in a given url (to the constructor) and puts data
        into a BeautifulSoup context.
        """
        ua_string = 'Content-Audit/2.0'
        for article_url in self.article_urls:
            # print ("Parsing %s" % line_url)
            self.url_parts = urllib.parse.urlparse(article_url)
            req = urllib.request.Request(article_url)
            req.add_header('User-Agent', ua_string)
            try:
                data = urllib.request.urlopen(req, timeout = 10)
            except:
                continue
            self.soupy_data = BeautifulSoup(data, features="html.parser")
            try:
                self.extract_tags(article_url)
            except:
                continue
            time.sleep(random.uniform(1, 3))
        print("End of extraction")


    def extract_tags(self, article_url):
        """
        Searches through self.soupy_data and extracts meta tags such as page
        description and title for inclusion into content audit spreadsheet
        """
        page_info = {}
        page_info['mention_identifier'] = article_url

        for tag in self.soupy_data.find_all('meta', attrs={"name": True}):
            try:
                page_info[tag['name']] = tag['content']
            except:
                page_info[tag['name']] = ''
        page_info['title'] = self.soupy_data.head.title.contents[0]
        page_info['filename'] = self.url_parts[2]
        try:
            page_info['name'] = self.soupy_data.h3.get_text()
        except:
            page_info['name'] = ''
        self.add_necessary_tags(page_info, ['keywords', 'description', 'title'])
        self.site_info.append(page_info)
        self.soupy_data = ""


    def write_to_spreadsheet(self, filename):
        """
        Write data from self.meta_info to spreadsheet. 
        """

        path_to_csv_export = filename.split(".")[0] + "." + filename.split(".")[1] + ".enhanced.csv"
        
        # open the file in the write mode
        self.csv_output = open("mine_mention_tags.csv", 'w')

        # create the csv writer
        writer = csv.writer(self.csv_output, quoting=csv.QUOTE_NONNUMERIC)

        # write header row to the csv file
        row = ['mention_identifier', 'page_name', 'file_name', 'page_title', 'page_description', 'keywords']
        writer.writerow(row)

        # write meta data
        for dex in self.site_info:
            row = [
                dex['mention_identifier'], 
                dex['name'], 
                dex['filename'],
                dex['title'],
                dex['description'],
                dex['keywords']
            ]
            writer.writerow(row)

        # close the file
        self.csv_output.close()
        
        # Save to S3
        s3 = boto3.resource('s3')
        s3.Bucket("discursus-io").upload_file("mine_mention_tags.csv", path_to_csv_export)
        os.remove("mine_mention_tags.csv")


    def add_necessary_tags(self, info_dict, needed_tags):
        """
        This method insures that missing tags have a null value
        before they are written to the output spreadhseet.
        """
        for key in needed_tags:
            if key not in info_dict:
                info_dict[key] = " "
        return info_dict


@op
def enhance_mentions(context, gdelt_events_miner_results):
    filename = gdelt_events_miner_results.splitlines()[-1]
    context.log.info("Enhancing " + filename)

    content_bot = ContentAuditor(filename)
    content_bot.get_list_of_urls()

    context.log.info("Mining " + str(len(content_bot.article_urls)) + " articles")
    content_bot.read_url()

    context.log.info("Exporting to S3")
    content_bot.write_to_spreadsheet(filename)