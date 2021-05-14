import pandas as pd


def team(match, monster_event):
    team = pd.DataFrame(match['teams'][0])
    
    team_columns = ['gameId', 'teamId', 'win', 'ban1', 'ban2', 'ban3', 'ban4', 'ban5',
                    'dragon1', 'dragon2', 'dragon3', 'dragon4', 'dragon5', 'dragon6', 'dragon7',
                    'firstBlood', 'firstTower', 'firstDragon', 'firstBaron', 'firstHerald', 'firstInhibitor',
                    'totalTower', 'totalDragon', 'totalBaron', 'totalHerald', 'totalInhibitor']

    team_df = pd.DataFrame(index=[0,1], columns=team_columns)

    team_use = team.drop(['vilemawKills', 'dominionVictoryScore', 'bans'], axis=1)
    rename_columns = {'firstRiftHerald': 'firstHerald', 'towerKills': 'totalTower', 'dragonKills': 'totalDragon',
                      'baronKills': 'totalBaron', 'riftHeraldKills': 'totalHerald', 'inhibitorKills': 'totalInhibitor'}
    team_use = team_use.rename(columns=rename_columns)
    
    
    champ = pd.read_csv('../data_csv/champ.csv')
    champ_dict = dict(zip(champ['championId'], champ['champion']))
    champ_dict[-1] = 'NO BAN'
    
    ban_list = [[None for _ in range(5)], [None for _ in range(5)]]
    for j in range(2):
        for i in range(5):
            if team['bans'].iloc[j][i]['pickTurn'] < 6:
                pick_index = team['bans'].iloc[j][i]['pickTurn'] - 1
                ban_list[0][pick_index] = champ_dict[team['bans'].iloc[j][i]['championId']]
            else:
                pick_index = team['bans'].iloc[j][i]['pickTurn'] - 6
                ban_list[1][pick_index] = champ_dict[team['bans'].iloc[j][i]['championId']]

    
    dragon_list = [[], []]
    
    for i in range(len(monster_event)):
        one_event = monster_event.iloc[i]
        if one_event['monsterType'] == 'DRAGON':
            if one_event['teamId'] == 'BLUE':
                dragon_list[0].append(one_event['dragonSubType'])
            else:
                dragon_list[1].append(one_event['dragonSubType'])

                
                
    dragon_list[0] += [None]*(7-len(dragon_list[0]))
    dragon_list[1] += [None]*(7-len(dragon_list[1]))
    
    
    team_df['gameId'] = match['gameId'].item()
    team_df[list(team_use.columns)] = team_use
    team_df.loc[0, 'ban1':'ban5'] = ban_list[0]
    team_df.loc[1, 'ban1':'ban5'] = ban_list[1]
    team_df.loc[0, 'dragon1':'dragon7'] = dragon_list[0]
    team_df.loc[1, 'dragon1':'dragon7'] = dragon_list[1]
    
    
    return team_df
