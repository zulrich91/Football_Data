import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import sys, getopt
import csv
import unidecode

HOME_PAGE = "https://fbref.com/"
BASE_MATCH_LOG_URL = "https://fbref.com/en/players/19cda00b/matchlogs/2021-2022/summary/Angel-Di-Maria-Match-Logs"
BASE_PLAYER_URL = "https://fbref.com/en/players/"

def unidecodePlayersName(name):
    players_name = unidecode.unidecode(name)
    return players_name

def joined_player_name(name):
    return "-".join(name.split(" "))

def generatePlayersUrl(id, name):
    players_name= unidecodePlayersName(name)
    players_name = joined_player_name(players_name)
    url = BASE_PLAYER_URL + id + "/" + players_name
    return url

def generatePlayerMatchLogsUrl(id,name,path):
    players_name= unidecodePlayersName(name)
    log_name = joined_player_name(players_name) + "-Match-Logs"
    matchLogUrl = BASE_PLAYER_URL + id + "/matchlogs/" + path.split("/")[-3] + "/summary/" + log_name
    return matchLogUrl
def check_field(field, value, player_dict):
    if field in player_dict:
        player_dict[field].append(value)
    else:
        player_dict[field] = [value]
    return player_dict

def get_date(row):
    date = row.find('th',{"scope":"row"}).find("a").text
    date_url = row.find('th',{"scope":"row"}).find("a", href=True).attrs['href']
    return date, date_url

def get_text(cell):
    a = cell.text.strip().encode()
    text=a.decode("utf-8")
    return text

def get_comp_roud_opponent(cell):
    try:
        text = cell.find('a', href=True).text
    except Exception as e:
        # print(e)
        text=''
    return text

def get_url(cell):
    try:
        return cell.find('a', href=True).attrs['href']
    except Exception as e:
        # print(e)
        return ''

#Functions to get the data in a dataframe using BeautifulSoup
def get_tables(url):
    res = requests.get(url)
    ## The next two lines get around the issue with comments breaking the parsing.
    comm = re.compile("<!--|-->")
    soup = BeautifulSoup(comm.sub("",res.text),"html.parser")
    all_tables = soup.findAll("tbody")
    return all_tables, url.split("/")[-1].replace("-Match-Logs", "")

def get_frame(features, player_table,player):
    pre_df_player = dict()
    pre_df_player['player'] = player
    features_wanted_player = features
    rows_player = player_table.find_all('tr')
    for row in rows_player:
        if(row.find('th',{"scope":"row"}) != None):
            try:
                date, date_url = get_date(row)     
            except Exception as e:
                date, date_url = '',''
                # print(e)
            pre_df_player = check_field("date",date, pre_df_player)
            pre_df_player = check_field("date_url",date_url, pre_df_player)

            for f in features_wanted_player:
                cell = ''
                text = ''
                try:
                    cell = row.find("td",{"data-stat": f})
                    text = get_text(cell)
                except Exception as e:
                    text = ''
                    # print(e)

                if ((f == "comp") or (f == "round") or (f == "opponent")):
                    text= get_comp_roud_opponent(cell)
                
                if (f == "match_report"):
                    text =  get_url(cell)

                if (f == "opponent"):
                    opponent_url = get_url(cell)
                    pre_df_player = check_field("opponent_url", opponent_url, pre_df_player)

                if (f == "squad"):
                    squad_url = get_url(cell)
                    pre_df_player = check_field("squad_url", squad_url, pre_df_player)

                if (f == "comp"):
                    comp_url = get_url(cell)
                    pre_df_player = check_field("comp_url", comp_url, pre_df_player)

                if (f == "round"):
                    round_url = get_url(cell)
                    pre_df_player = check_field("round_url", round_url, pre_df_player)

                if(text == ''):
                    text = '0'
                if((f!='match_report')&(f!='dayofweek')&(f!='comp')&(f!='round')&(f!='venue')\
                    &(f!='result')&(f!='matches')&(f!='squad')&(f!='opponent')&(f!='game_started')&(f!='position')):
                    text = float(text.replace(',',''))
                if f in pre_df_player:
                    pre_df_player[f].append(text)
                else:
                    pre_df_player[f] = [text]
            
    df_player = pd.DataFrame.from_dict(pre_df_player)
    return df_player