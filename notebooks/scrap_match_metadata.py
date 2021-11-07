import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import sys, getopt
import csv
from utils import *
import time

COUNTER = 0
columns = [ 'player', 'date', 'date_url', 'dayofweek', 'comp_url', 'comp',
            'round_url', 'round', 'venue', 'result', 'squad_url', 'squad',
            'opponent_url', 'opponent', 'game_started', 'position', 'minutes',
            'goals', 'assists', 'pens_made', 'pens_att', 'shots_total',
            'shots_on_target', 'cards_yellow', 'cards_red', 'touches', 'pressures',
            'tackles', 'interceptions', 'blocks', 'xg', 'npxg', 'xa', 'sca', 'gca',
            'passes_completed', 'passes', 'passes_pct', 'progressive_passes',
            'carries', 'progressive_carries', 'dribbles_completed', 'dribbles',
            'match_report'
           ]
problematic_logs = []

def get_venue_attendance_referees(url):
    global problematic_logs
    try:
        result_div = get_match_metadata(url)
    except Exception as e:
        problematic_logs.append(url)
        return "","","","","","",""

    try:
        divs = result_div.find('div', class_="scorebox_meta")
    except Exception as e:
        problematic_logs.append(url)
        return "","","","","","",""
    
    try:
        venue = divs.find_all("small")[3].text
    except Exception as e:
        venue = '' 

    try:
        attendance = divs.find_all("small")[1].text
    except Exception as e:
        attendance = ""

    try:
        referee = unidecodePlayersName(divs.find_all("small")[5].find_all("span")[0].text).split(" (")[0]
    except Exception as e:
        referee=''
        
    try:
        AR1 = unidecodePlayersName(divs.find_all("small")[5].find_all("span")[1].text).split(" (")[0]
    except Exception as e:
        AR1 =''
    try:

        AR2 = unidecodePlayersName(divs.find_all("small")[5].find_all("span")[2].text).split(" (")[0]
    except Exception as e:
        AR2=''
    try:
        fourth = unidecodePlayersName(divs.find_all("small")[5].find_all("span")[3].text).split(" (")[0]
    except Exception as e:
        fourth=''
    try:
        venue_time = divs.find("span", {"class":"venuetime"}).text.split(" (")[0]
    except Exception as e:
        venue_time=''
    return [venue,attendance,referee, AR1, AR2,fourth,venue_time]

def get_managers_and_captains(url):
    global problematic_logs
    try:
        result_div = get_match_metadata(url)
    except Exception as e:
        problematic_logs.append(url)
        return "","","","","",""
    try:
        divs = result_div.find_all('div', class_='datapoint')
    except Exception as e:
        problematic_logs.append(url)
        return "","","","","",""
    try:
        manager = unidecodePlayersName (divs[0].get_text().split(": ")[1].replace("</div>",''))
    except Exception as e:
        manager =''

    try:
        captain = unidecodePlayersName( divs[1].find("a", href=True).text)
    except Exception as e:
        captain =''

    try:
        captain_url = divs[1].find("a", href=True).attrs["href"]
    except Exception as e: 
        captain_url =''
    try:
        opponent_manager = unidecodePlayersName (divs[2].get_text().split(": ")[1].replace("</div>",''))
    except Exception as e: 
        opponent_manager=''
    try:
        opponent_captain = unidecodePlayersName (divs[3].find("a", href=True).text)
    except Exception as e:
        opponent_captain =''
    try:

        opponent_captain_url = divs[3].find("a", href=True).attrs["href"]
    except Exception as e:
        opponent_captain_url = ''
    
    return [manager,captain,captain_url, opponent_manager, opponent_captain, opponent_captain_url]

if __name__ == "__main__":
    START_TIME = time.time()
    try:
        result_df = pd.read_csv("match_metadata.csv", header=0)
        print("Found an existing csv and using it in append mode ---->\n")
    except Exception as e:
        print("Initializing a new dataframe ----->")
        result_df = pd.DataFrame(columns=columns)

    # for chunk in pd.read_csv("sample_match_logs_to_get_metadata.csv", chunksize=10):
    for chunk in pd.read_csv("../data/players_match_details.csv", chunksize=10):

        try:
            chunk[['manager','captain','captain_url', 'opponent_manager', 'opponent_captain', 'opponent_captain_url']] = \
            chunk.apply(lambda x : get_managers_and_captains(x['match_report']), axis=1, result_type="expand")
        except Exception as e:
            print(e)
            chunk[['manager','captain','captain_url', 'opponent_manager', 'opponent_captain', 'opponent_captain_url']] = ['','','','','','']

        try:
            chunk[['venue','attendance','referee', 'AR1', 'AR2','fourth','venue_time']] = \
            chunk.apply(lambda x : get_venue_attendance_referees(x['match_report']), axis=1, result_type="expand")
        except Exception as e:
            print(e)
            chunk[['venue','attendance','referee', 'AR1', 'AR2','fourth','venue_time']] = ['','','','','','','']
        
        try:
            result_df = pd.concat([result_df, chunk],axis=0)
            result_df.to_csv("match_metadata.csv", index=False)
        except Exception as e:
            print(e)
        COUNTER+=20
        print("\nCompleted : {}\nTotal execution time : {} seconds\n".format(COUNTER, time.time()-START_TIME))

    try:
        pd.DataFrame({'url':problematic_logs}).to_csv("probematic_match_metadata_logs.csv", index=False)
    except Exception as e:
        print(e)
    print('Completed Sucessfully !')
    
        


