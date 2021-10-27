import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import sys, getopt
import csv
import time 

COUNTER = 0
START_TIME = time.time()
df = pd.read_csv("../data/joueur_url.csv")

def get_match_logs(url):
    global COUNTER
    match_dico= {}
    res = requests.get(url)
    ## The next two lines get around the issue with comments breaking the parsing.
    comm = re.compile("<!--|-->")
    # soup = BeautifulSoup(comm.sub("",res.text),'lxml')
    soup = BeautifulSoup(comm.sub("",res.text),"html.parser")
    inner_div = soup.find("div", id="inner_nav")
    match_logs_years = inner_div.find_all("ul")[0].find_all("ul")[1]
    match_logs = match_logs_years.find_all("li")
    for match_log in match_logs:
    # print(match_log.find("a").get("href").split("/")[5])
    # print(match_log.text)
        match_dico[match_log.text] = match_log.find("a").get("href").split("/")[5]
    COUNTER+=1
    duration = time.time()-START_TIME
    print("{} : Done {}".format(COUNTER, url))
    print("Completed {}/{} \nExecution time {} seconds\n".format(COUNTER, df.shape[0], duration))

    return match_dico

if __name__ == "__main__":
    df["logs"] = df.url.apply(lambda x: get_match_logs(x))