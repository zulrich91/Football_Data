from utils import * 
import pandas as pd
import json
import ast


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

def scrap_match_urls(id, name, logs):
    # # logs = dict(json.loads(logs))
    # logs = ast.literal_eval(json.dumps(logs))
    print(logs)
    global result_df
    players_name= unidecodePlayersName(name)
    log_name = joined_player_name(players_name) + "-Match-Logs"

    for log in logs.values():
        matchLogUrl = BASE_PLAYER_URL + id + "/matchlogs/" + str(log) + "/summary/" + log_name
        print("Scrapping ->: {}".format(matchLogUrl))
        tables, player = get_tables(matchLogUrl)
        scrap_result_df = get_frame(fields,tables[0], player)
        if len(result_df) == 0 :
            result_df = scrap_result_df.copy(deep=True)
        else:
            result_df = pd.concat([result_df, scrap_result_df],axis=0)
    result_df.to_csv("final_scrapped_match_logs.csv", index=False)
    # return result_df

if __name__ == "__main__":
    for chunk in pd.read_csv("sample_logs_to_scrap.csv", chunksize=10):
        chunk.apply(lambda x: scrap_match_urls(x["id"], x['name'], eval(x['logs'])), axis=1)