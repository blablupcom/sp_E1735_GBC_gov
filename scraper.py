# -*- coding: utf-8 -*-

#### IMPORTS 1.0

import os
import re
import scraperwiki
import urllib2
from datetime import datetime
from bs4 import BeautifulSoup


#### FUNCTIONS 1.2
import requests

def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9QY][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9QY][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    now = datetime.now()
    year, month = date[:4], date[5:7]
    validYear = (2000 <= int(year) <= now.year)
    if 'Q' in date:
        validMonth = (month in ['Q0', 'Q1', 'Q2', 'Q3', 'Q4'])
    elif 'Y' in date:
        validMonth = (month in ['Y1'])
    else:
        try:
            validMonth = datetime.strptime(date, "%Y_%m") < now
        except:
            return False
    if all([validName, validYear, validMonth]):
        return True


def validateURL(url):
    try:
        r = urllib2.urlopen(url)
        count = 1
        while r.getcode() == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = urllib2.urlopen(url)
        sourceFilename = r.headers.get('Content-Disposition')
        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.getcode() == 200
        validFiletype = ext.lower() in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        print ("Error validating URL.")
        return False, False


def validate(filename, file_url):
    validFilename = validateFilename(filename)
    validURL, validFiletype = validateURL(file_url)
    if not validFilename:
        print filename, "*Error: Invalid filename*"
        print file_url
        return False
    if not validURL:
        print filename, "*Error: Invalid URL*"
        print file_url
        return False
    if not validFiletype:
        print filename, "*Error: Invalid filetype*"
        print file_url
        return False
    return True


def convert_mth_strings ( mth_string ):
    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    for k, v in month_numbers.items():
        mth_string = mth_string.replace(k, v)
    return mth_string


#### VARIABLES 1.0

entity_id = "E1735_GBC_gov"
url = "https://www.gosport.gov.uk/sections/your-council/transparency/invoices-over-500-pounds/"
errors = 0
data = []
ua={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}

#### READ HTML 1.0

import ssl
import sys

import requests

from requests.utils import urlparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

class TlsAdapter(HTTPAdapter):

    def __init__(self, ssl_options=0, **kwargs):
        self.ssl_options = ssl_options
        super(TlsAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, *pool_args, **pool_kwargs):
        ctx = ssl_.create_urllib3_context(ssl.PROTOCOL_TLS)
        # extend the default context options, which is to disable ssl2, ssl3
        # and ssl compression, see:
        # https://github.com/shazow/urllib3/blob/6a6cfe9/urllib3/util/ssl_.py#L241
        ctx.options |= self.ssl_options
        self.poolmanager = PoolManager(*pool_args,
                                       ssl_context=ctx,
                                       **pool_kwargs)

session = requests.session()
# disallow tls1.0 and tls1.1, allow only tls1.2 (and newer if suported by
# the used openssl version)
adapter = TlsAdapter(ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1)
session.mount("https://", adapter)
r = session.get(url)
print(r.status_code)


# html = requests.get(url, verify=False)
# soup = BeautifulSoup(html.text, 'lxml')

#### SCRAPE DATA

next_link = soup.find('span', attrs = {'class': 'pagination-next'}).find('a', href=True)
while next_link:
    ul_blocks = soup.find('table', attrs = {'class': 'DataGrid oDataGrid'}).find('tbody').find_all('tr')
    for ul_block in ul_blocks:
        pic = ul_block.find('img')['alt']
        if 'text/csv' in pic:
            file_name = ul_block.find_all('td')[1].text.strip()
            csvMth = file_name.split('-')[1].strip()[:3]
            csvYr = file_name.split('-')[1].strip().split()[1]
            if 'http' not in ul_block.find_all('td')[-1].find('a')['href']:
                url = 'https://www.gosport.gov.uk' + ul_block.find_all('td')[-1].find('a')['href']
            else:
                url = ul_block.find_all('td')[-1].find('a')['href']
            csvMth = convert_mth_strings(csvMth.upper())
            data.append([csvYr, csvMth, url])
    next_l = soup.find('span', attrs={'class': 'pagination-next'})
    if next_l:
        next_link =next_l.find('a', href=True)
        next_link = 'https://www.gosport.gov.uk/sections/your-council/transparency/invoices-over-500-pounds/'+next_link['href']
        next_html = requests.get(next_link, verify=False)
        soup = BeautifulSoup(next_html.text, 'lxml')
    else:
        break

#### STORE DATA 1.0

for row in data:
    csvYr, csvMth, url = row
    filename = entity_id + "_" + csvYr + "_" + csvMth
    todays_date = str(datetime.now())
    file_url = url.strip()

    valid = validate(filename, file_url)

    if valid == True:
        scraperwiki.sqlite.save(unique_keys=['l'], data={"l": file_url, "f": filename, "d": todays_date })
        print filename
    else:
        errors += 1

if errors > 0:
    raise Exception("%d errors occurred during scrape." % errors)


#### EOF
