#Python version 3.8.5 on Windows 10
#Collect data from insideairbnb.com by HTTP requests

import sys
import requests
from bs4 import BeautifulSoup

"""
download dataset from http://insideairbnb.com
"""
# main entry point
if __name__ == '__main__':
    fns = ("listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz", "listings.csv", "reviews.csv", "neighbourhoods.csv", "neighbourhoods.geojson")
    url = "http://insideairbnb.com/get-the-data.html"
    rmain = requests.get(url)
    soup = BeautifulSoup(rmain.content, 'html5lib')
    #print(soup.prettify())
    #loop for all links
    for link in soup.find_all('a'):
        href = link.get('href')
        temp = href.split('/')
        if temp[-1] in fns: 
            fn = temp[-4]+'-'+temp[-3]+'-'+temp[-1]
            r = requests.get(href, stream = True)   #download file
            with open(fn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)  #save file in the current directory
    sys.exit()
