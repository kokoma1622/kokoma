import pandas as pd

def minute(frame, kill_log):
    minutes = len(frame)
    
    index = [i for i in range(minutes)]
    columns = ['gameID', 'minute', 'kills', 'deaths', 'assists', 'minionCS', 'jungleCS', 'level', 'xp', 'totalGold', 'currentGold']
    minute_df = pd.DataFrame(index=index, columns=columns)
    
    for i in range(minutes):
        minute_data = pd.DataFrame(frame.iloc[i].values(), index=frame.iloc[i].keys())
        minionCS, jungleCS, level, xp, totalGold, currentGold = [None]*10, [None]*10, [None]*10, [None]*10, [None]*10, [None]*10

        for j in range(10):
            p_index = minute_data['participantId'][j] - 1
            minionCS[p_index] = str(minute_data['minionsKilled'][j])
            jungleCS[p_index] = str(minute_data['jungleMinionsKilled'][j])
            level[p_index] = str(minute_data['level'][j])
            xp[p_index] = str(minute_data['xp'][j])
            totalGold[p_index] = str(minute_data['totalGold'][j])
            currentGold[p_index] = str(minute_data['currentGold'][j])

        minute_df.loc[i,'minute'] = i
        minute_data = [' '.join(minionCS), ' '.join(jungleCS), ' '.join(level), ' '.join(xp), ' '.join(totalGold), ' '.join(currentGold)]
        minute_df.loc[i,'minionCS':'currentGold'] = minute_data
    
    kills_time_list = [[0] * 10 for _ in range(minutes)]
    deaths_time_list = [[0] * 10 for _ in range(minutes)]
    assist_time_list = [[0] * 10 for _ in range(minutes)]

    for i in range(len(kill_log)):
        kill_min = min(kill_log.loc[i,'timestamp'] // 60000 + 1, minutes-1)
        kills_time_list[kill_min][kill_log.loc[i,'participantID'] - 1] += 1
        deaths_time_list[kill_min][kill_log.loc[i,'victimID'] - 1] += 1

        assistList = kill_log.loc[i,'assistIDs'].split()
        for assist in assistList:
            assist_time_list[kill_min][int(assist)-1] += 1
    
    
    kills_list = [[0] * 10 for _ in range(minutes)]
    deaths_list = [[0] * 10 for _ in range(minutes)]
    assist_list = [[0] * 10 for _ in range(minutes)]
    
    for i in range(minutes-1):
        for j in range(10):
            kills_list[i+1][j] = kills_list[i][j] + kills_time_list[i+1][j]
            deaths_list[i+1][j] = deaths_list[i][j] + deaths_time_list[i+1][j]
            assist_list[i+1][j] = assist_list[i][j] + assist_time_list[i+1][j]
        
        
    kills_in = []
    deaths_in = []
    assist_in = []

    for i in range(minutes):
        kills_in.append(' '.join(map(str,kills_list[i])))
        deaths_in.append(' '.join(map(str,deaths_list[i])))
        assist_in.append(' '.join(map(str,assist_list[i])))
            
    minute_df.loc[:,'kills'] = kills_in
    minute_df.loc[:,'deaths'] = deaths_in
    minute_df.loc[:,'assists'] = assist_in
    
    
    return minute_df
