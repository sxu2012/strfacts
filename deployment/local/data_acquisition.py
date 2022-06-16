#!/usr/bin/env python
# coding: utf-8

## Converted from: Jupyter Notebook PySpark with Python3 on Azure Databricks
## Collect data from insideairbnb.com by HTTP requests; This data is
## licensed under a Creative Commons Attribution 4.0 International License:
## https://creativecommons.org/licenses/by/4.0/legalcode

'''
Data Acquition:
    Collect data from insideairbnb.com by HTTP requests

    Input: Files from insiderairbnb.com in csv zipped .gz format
    Output: Store files to local or Azure Blob containers folder indata
            with subfolders yyyy-mm (such as 2022-03)
'''

import sys
import os
import requests
from bs4 import BeautifulSoup


def mount_blob():
    return

    storageAccountName = 'strfactsblob1'
    storageAccountAccessKey = 'tFH01V67MM+n4fP1SAOIiDVDJcvK6fLRwGYygrUuP1huddfNNFUMbS9oFg8Ff2OGRQvMF4oLoBqQ+AStT3+1KQ=='
    ContainerName = 'bcontainer1'
 
    if not any(mount.mountPoint == '/mnt/blob1/' for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(ContainerName, storageAccountName),
            mount_point = "/mnt/blob1",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
        except Exception as e:
            print('already mounted')

def download(indata):

    fns = ("listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz", "listings.csv", "reviews.csv", "neighbourhoods.csv", "neighbourhoods.geojson")

    url = "http://insideairbnb.com/get-the-data.html"
    rmain = requests.get(url)
    if indata == "c:/sb/strfacts/indata": 
        soup = BeautifulSoup(rmain.content, 'html5lib')
    else:
        soup = BeautifulSoup(rmain.content, 'html.parser')
    #print(soup.prettify())
    #loop for all links
    for link in soup.find_all('a'):
        href = link.get('href')
        if href is None:
            continue    #skip to the next loop
        temp = href.split('/')
        #temp[-1] - calendar.csv.gz
        #temp[-3] - 2021-12-05
        #temp[-4] - hawaii
        if temp[-1] in fns: 
            #ym - yyyy-mm
            ym = temp[-3][0:7]
            #test if folder already exists
            dn = indata+'/'+ym
            if not os.path.exists(dn):
                os.makedirs(dn)
            fn = dn+'/'+temp[-4]+'-'+temp[-1]
            #test if file exists, skip to the next file
            if os.path.exists(fn):
                continue
            print(fn)
            r = requests.get(href, stream = True)   #download file
            with open(fn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)  #save file

# main entry point
if __name__=="__main__":
    if len(sys.argv) >= 2:
        # for running in local Windows 11
        download("c:/sb/strfacts/indata")
    else:
        # default to run on Azure Databricks
        mount_blob()
        download("/dbfs/mnt/blob1/indata")

