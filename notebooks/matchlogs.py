from numpy.core.fromnumeric import size
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import sys, getopt
import csv
import time 

COUNTER =0
START_TIME = time.time()
result_df = pd.DataFrame(columns=['id', 'name', 'url', 'match_logs','logs'])
error_df = pd.DataFrame(columns=["url"])
problematic_urls = []
CURRENT_URL = ""

def get_match_logs(url):
    global COUNTER
    global CURRENT_URL
    match_dico= {}
    try:
        print("Scraping URL -> {}".format(url))
        res = requests.get(url)
        ## The next two lines get around the issue with comments breaking the parsing.
        comm = re.compile("<!--|-->")
        soup = BeautifulSoup(comm.sub("",res.text),"html.parser")
        inner_div = soup.find("div", id="inner_nav")
        match_logs_years = inner_div.find_all("ul")[0].find_all("ul")[1]
        match_logs = match_logs_years.find_all("li")
        for match_log in match_logs:
            match_dico[match_log.text] = match_log.find("a").get("href").split("/")[5]
        duration = time.time()-START_TIME
        COUNTER+=1
        print("Completed {}/16071 \nTotal Execution time {} seconds\n".format(COUNTER, duration))
        return match_dico
    except Exception as e:
        print("Exception ->: {}\nConcerning Player ->: {}".format(e, url))
        CURRENT_URL = url
        problematic_urls.append(url)
        error_df['url']=problematic_urls
        error_df.to_csv("problematic_urls1.csv", index=False)
        return 'None'

if __name__ == "__main__":
    for chunk in pd.read_csv("../data/partition1.csv", chunksize=10):
        try:
            print("Scrapping new chunk ----->")
            chunk['logs']=chunk.url.apply(lambda x: get_match_logs(x))
            if len(result_df) == 0 :
                print("Scrap Started")
                result_df = chunk.copy(deep=True)
                result_df.to_csv("../data/matchlogs2.csv", index=False)
            else:
                result_df = pd.concat([result_df,chunk],axis=0)
                result_df.to_csv("../data/matchlogs2.csv", index=False)
        except Exception as e :
            print("Exception ->: {}\nConcerning Player ->: {}".format(e, CURRENT_URL))
    print("Scrap completed successfully")

