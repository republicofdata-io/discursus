# Credit goes to...
# https://github.com/quakerpunk/content-audit/blob/origin/content_audit.py

# from dagster import solid

from bs4 import BeautifulSoup
from optparse import OptionParser
import urllib.request
import urllib.error
import random
import csv
import urllib.parse
import re
import time

class ContentAuditor:
    """
    ContentAuditor
    This script takes a list of URLs and retrieves data such as meta tags
    containing keywords, description and the page title. It then populates a
    spreadsheet with the data for easy review.
    """

    def __init__(self, filename):
        """
        Initialization method for the ContentAuditor class.
        requirements:
        BeautifulSoup for HTML parsing
        xlwt for writing to an Excel spreadsheet without use of COM Interop
        """
        self.filehandle = open(filename, 'r')
        self.soupy_data = ""
        self.csv_output = ""
        self.content = ""
        self.text_file = ""
        self.site_info = []
        self.url_parts = ""
        self.reg_expres = re.compile(r"www.(.+?)(.com|.net|.org)")

    def read_url(self):
        """
        read_url
        Method which reads in a given url (to the constructor) and puts data
        into a BeautifulSoup context.
        We begin setting a string for the user-agent. Checking for comment
        lines in the URL list, we take a web address, one at a time, download
        the HTML, parse it with BeautifulSoup then pass it off to extract tags.
        Along the way, we check for any connectivity or remote server issues
        and handle them appropriately.
        """
        right_now = str(int(time.time()))
        ua_string = 'Content-Audit/2.0'
        for line in self.filehandle:
            line_url = line.split("\t")[5]
            if line.startswith("#"):
                continue
            # print ("Parsing %s" % line_url)
            self.url_parts = urllib.parse.urlparse(line_url)
            req = urllib.request.Request(line_url)
            req.add_header('User-Agent', ua_string)
            try:
                data = urllib.request.urlopen(req)
            except urllib.error.HTTPError as ex:
                continue
            except urllib.error.URLError as urlex:
                continue
            self.soupy_data = BeautifulSoup(data, features="html.parser")
            try:
                self.extract_tags()
            except:
                continue
            time.sleep(random.uniform(1, 3))
        print("End of extraction")

    #Extraction methods

    def extract_tags(self):
        """
        extract_tags
        Searches through self.soupy_data and extracts meta tags such as page
        description and title for inclusion into content audit spreadsheet
        """
        page_info = {}

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

    #Spreadsheet methods

    def write_to_spreadsheet(self):
        """
        write_to_spreadsheet
        Write data from self.meta_info to spreadsheet. 
        """
        
        # open the file in the write mode
        self.csv_output = open(options.output, 'w')

        # create the csv writer
        writer = csv.writer(self.csv_output)

        # write header row to the csv file
        row = ['page_name', 'file_name', 'page_title', 'page_description', 'keywords']
        writer.writerow(row)

        # write meta data
        for dex in self.site_info:
            row = [
                dex['name'], 
                dex['filename'],
                dex['title'],
                dex['description'],
                dex['keywords']
            ]
            writer.writerow(row)

        # close the file
        self.csv_output.close()

    #Helper methods

    def add_necessary_tags(self, info_dict, needed_tags):
        """
        add_necessary_tags
        This method insures that missing tags have a null value
        before they are written to the output spreadhseet.
        """
        for key in needed_tags:
            if key not in info_dict:
                info_dict[key] = " "
        return info_dict


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="Filename containing URLs", metavar="FILE")
    parser.add_option("-o", "--output", dest="output",
                      help="Output file, usually a spreadsheet")

    (options, args) = parser.parse_args()

    if not options.output:
        parser.error("You did not specify an output file.")

    if options.filename:
        content_bot = ContentAuditor(options.filename)
        content_bot.read_url()
        content_bot.write_to_spreadsheet()
    else:
        parser.error("You did not specify an input file (a list of URLs)")
    

# @solid(required_resource_keys = {"snowflake"})
# def enhance_mentions(context, gdelt_mentions_miner_results):
#     context.log.info(gdelt_mentions_miner_results.splitlines()[-1])