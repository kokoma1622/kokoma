import pandas as pd
from copy import deepcopy

def get_ts(team, champions, kills, monster_event):
    team_columns = ['gameid', 'team', 'win', 'champions', 'bans', 'firsts', 'totals', 'dragons']

    team_df = pd.DataFrame(index=[0,1], columns=team_columns)
    
    team_df['team'] = ['BLUE', 'RED']
    win_dict = {'Win':'VICTORY', 'Fail':'DEFEAT'}
    team_df['win'] = [win_dict[team['win'][0]], win_dict[team['win'][1]]]
    
    team_df['champions'] = [champions[:5], champions[5:]]
    team_df['bans'] = [pd.DataFrame(team['bans'][0])['championId'].tolist(), pd.DataFrame(team['bans'][1])['championId'].tolist()]
    
    firsts = ['firstBlood', 'firstTower', 'firstInhibitor', 'firstDragon', 'firstRiftHerald', 'firstBaron']
    bool_dict = {0:False}
    team_df['firsts'] = team[firsts].replace(bool_dict).values.tolist()
    
    team['kills'] = [sum(kills[:5]), sum(kills[5:])]
    totals = ['kills', 'towerKills', 'inhibitorKills', 'dragonKills', 'riftHeraldKills', 'baronKills']
    team_df['totals'] = team[totals].values.tolist()    
    
    
    dragon_list = [[], []]
    for i in range(len(monster_event)):
        one_event = monster_event.iloc[i]
        if one_event['monster'] not in  ['HERALD','BARON']:
            if one_event['team'] == 'BLUE': dragon_list[0].append(one_event['monster'].split('_')[1])
            else:                           dragon_list[1].append(one_event['monster'].split('_')[1])
    team_df.loc[:,'dragons'] = dragon_list
    
    
    return team_df
