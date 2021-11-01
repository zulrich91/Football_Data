from utils import * 
import pandas as pd
import json
import ast
import time 


fields = [  "dayofweek", "comp","round","venue","result", "squad", "opponent","game_started", 
            "position", "minutes",'goals', 'assists', 'pens_made', 'pens_att', 'shots_total', 
            "shots_on_target", "cards_yellow", "cards_red", "touches", "pressures",
            'tackles', 'interceptions', 'blocks','xg', 'npxg', 'xa', 'sca', 'gca', 'passes_completed', 
            'passes', 'passes_pct','progressive_passes', 'carries', "progressive_carries", 
            'dribbles_completed', 'dribbles', 'match_report'
         ]
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
result_df = pd.DataFrame(columns=columns)
problematic_logs = []
current_log = ''
counter = 0
START_TIME = time.time()

def scrap_match_urls(id, name, logs):
    global result_df
    global current_log
    global problematic_logs
    global counter 
    try: 
        current_log = str(id)+"/"+str(logs)
        players_name= unidecodePlayersName(name)
        log_name = joined_player_name(players_name) + "-Match-Logs"

        for log in logs.values():
            matchLogUrl = BASE_PLAYER_URL + str(id) + "/matchlogs/" + str(log) + "/summary/" + log_name
            current_log = matchLogUrl
            print("Scrapping ->: {} ".format(matchLogUrl))
            tables, player = get_tables(matchLogUrl)
            scrap_result_df = get_frame(fields,tables[0], player)
            if len(result_df) == 0 :
                result_df = scrap_result_df.copy(deep=True)
            else:
                result_df = pd.concat([result_df, scrap_result_df],axis=0)
            counter=counter+1
            print("Completed {} \nTotal Execution time {} seconds\n".format(counter, time.time()-START_TIME))
        result_df.to_csv("../data/players_match_details.csv", index=False)

    except Exception as e:
        print("Exception ->: {}\nConcering player with log(s) ->: {} ".format(e, str(current_log)))
        problematic_logs.append(current_log)
        pd.DataFrame({'url':problematic_logs}).to_csv("probematic_logs.csv", index=False)
        counter=counter+1
        print("Completed {} \nTotal Execution time {} seconds\n".format(counter, time.time()-START_TIME))

if __name__ == "__main__":
    try:
        for chunk in pd.read_csv("../data/all_match_logs_to_scrap.csv", chunksize=10):
            chunk.apply(lambda x: scrap_match_urls(x["id"], x['name'], eval(x['logs'])), axis=1)
    except Exception as e:
        print("Exception ->: {}\nConcering player with logs ->: {} ".format(e, str(current_log)))
        problematic_logs.append(current_log)
    print("Completed Successfully!!!")
    pd.DataFrame({'url':problematic_logs}).to_csv("probematic_logs.csv", index=False)