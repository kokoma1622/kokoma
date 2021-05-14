import pandas as pd


def team(match, monster_event):
    team = pd.DataFrame(match['teams'][0])
    
    team_columns = ['gameID', 'teamID', 'win', 'champ1', 'champ2', 'champ3', 'champ4', 'champ5', 
                    'ban1', 'ban2', 'ban3', 'ban4', 'ban5', 'dragon',
                    'firstBlood', 'firstTower', 'firstDragon', 'firstBaron', 'firstHerald', 'firstInhibitor',
                    'totalTower', 'totalDragon', 'totalBaron', 'totalHerald', 'totalInhibitor']

    team_df = pd.DataFrame(index=[0,1], columns=team_columns)

    team_use = team.drop(['teamId', 'win', 'vilemawKills', 'dominionVictoryScore', 'bans'], axis=1)
    rename_columns = {'firstRiftHerald': 'firstHerald', 'towerKills': 'totalTower', 'dragonKills': 'totalDragon',
                      'baronKills': 'totalBaron', 'riftHeraldKills': 'totalHerald', 'inhibitorKills': 'totalInhibitor'}
    team_use = team_use.rename(columns=rename_columns)
    
    
    champ = pd.read_csv('../data_csv/champ.csv')
    champ_dict = dict(zip(champ['championId'], champ['champion']))
    champ_dict[-1] = 'NO BAN'
    
    
    participants = pd.DataFrame(match['participants'][0])
    pick_list = [None for _ in range(10)]
    for i in range(10):
        pick_index = participants.loc[i,'participantId'] - 1
        pick_list[pick_index] = champ_dict[participants.loc[i,'championId']]
    
    ban_list = [[None for _ in range(5)], [None for _ in range(5)]]
    for j in range(2):
        for i in range(5):
            if team['bans'].iloc[j][i]['pickTurn'] < 6:
                pick_index = team['bans'].iloc[j][i]['pickTurn'] - 1
                ban_list[0][pick_index] = champ_dict[team['bans'].iloc[j][i]['championId']]
            else:
                pick_index = team['bans'].iloc[j][i]['pickTurn'] - 6
                ban_list[1][pick_index] = champ_dict[team['bans'].iloc[j][i]['championId']]

    
    dragon_list = ['', '']
    
    for i in range(len(monster_event)):
        one_event = monster_event.iloc[i]
        if one_event['monsterType'] == 'DRAGON':
            if one_event['teamID'] == 'BLUE':
                dragon_list[0] += (one_event['dragonSubType'].split('_')[0] + ' ')
            else:
                dragon_list[1] += (one_event['dragonSubType'].split('_')[0] + ' ')
       
    dragon_list[0] = dragon_list[0].rstrip()
    dragon_list[1] = dragon_list[1].rstrip()
    
    team_dict = {100:'BLUE', 200:'RED'}
    win_dict = {'Win':'VICTORY', 'Fail':'DEFEAT'}
    
    team_df['gameID'] = match['gameId'].item()
    team_df.loc[0,'teamID'] = team_dict[team['teamId'][0]]
    team_df.loc[1,'teamID'] = team_dict[team['teamId'][1]]
    team_df.loc[0,'win'] = win_dict[team['win'][0]]
    team_df.loc[1,'win'] = win_dict[team['win'][1]]
    team_df[list(team_use.columns)] = team_use
    team_df.loc[0, 'champ1':'champ5'] = pick_list[:5]
    team_df.loc[1, 'champ1':'champ5'] = pick_list[5:]
    team_df.loc[0, 'ban1':'ban5'] = ban_list[0]
    team_df.loc[1, 'ban1':'ban5'] = ban_list[1]
    team_df.loc[:, 'dragon'] = dragon_list
    
    
    return team_df
