import pandas as pd

def timeline(frame, kill_log):
    minutes = len(frame)
    tenminutes = (minutes-1)//10 + 1
    
    index = [i for i in range(tenminutes*10)]
    columns = ['participantId', 'tenMinute', 'kills', 'deaths', 'assists', 'minionCS', 'jungleCS', 'level', 'xp', 'totalGold', 'currentGold']
    participants = [i for i in range(1,11)]
    time_df = pd.DataFrame(index=index, columns=columns)
    
    for i in range(tenminutes):
        frame_sub_df = frame.iloc[10*i:min(10*(i+1),minutes)]
        minionCS, jungleCS, level, xp, totalGold, currentGold = ['']*10, ['']*10, ['']*10, ['']*10, ['']*10, ['']*10
        
        for j in range(len(frame_sub_df)):
            minute_data = pd.DataFrame(frame_sub_df.iloc[j].values(), index=frame_sub_df.iloc[j].keys())
            
            for k in range(10):
                p_index = minute_data['participantId'][k] - 1
                minionCS[p_index] += (str(minute_data['minionsKilled'][k]) + ' ')
                jungleCS[p_index] += (str(minute_data['jungleMinionsKilled'][k]) + ' ')
                level[p_index] += (str(minute_data['level'][k]) + ' ')
                xp[p_index] += (str(minute_data['xp'][k]) + ' ')
                totalGold[p_index] += (str(minute_data['totalGold'][k]) + ' ')
                currentGold[p_index] += (str(minute_data['currentGold'][k]) + ' ')

        time_df.loc[10*i:10*i+9,'participantId'] = participants
        time_df.loc[10*i:10*i+9,'tenMinute'] = i*10
        time_df.loc[10*i:10*i+9,'minionCS'] = minionCS
        time_df.loc[10*i:10*i+9,'jungleCS'] = jungleCS
        time_df.loc[10*i:10*i+9,'level'] = level
        time_df.loc[10*i:10*i+9,'xp'] = xp
        time_df.loc[10*i:10*i+9,'totalGold'] = totalGold
        time_df.loc[10*i:10*i+9,'currentGold'] = currentGold
        
    
    kills_time_list = [[0] * minutes for _ in range(10)]
    deaths_time_list = [[0] * minutes for _ in range(10)]
    assist_time_list = [[0] * minutes for _ in range(10)]

    for i in range(len(kill_log)):
        kill_min = min(kill_log.loc[i,'timestamp'] // 60000 + 1, minutes-1)
        kills_time_list[kill_log.loc[i,'participantID'] - 1][kill_min] += 1
        deaths_time_list[kill_log.loc[i,'victimID'] - 1][kill_min] += 1

        assistList = kill_log.loc[i,'assistIDs'].split()
        for assist in assistList:
            assist_time_list[int(assist)-1][kill_min] += 1
    
    
    kills_list = [[0] * minutes for _ in range(10)]
    deaths_list = [[0] * minutes for _ in range(10)]
    assist_list = [[0] * minutes for _ in range(10)]
    
    for i in range(minutes-1):
        for j in range(10):
            kills_list[j][i+1] = kills_list[j][i] + kills_time_list[j][i+1]
            deaths_list[j][i+1] = deaths_list[j][i] + deaths_time_list[j][i+1]
            assist_list[j][i+1] = assist_list[j][i] + assist_time_list[j][i+1]
        
        
    kills_in = []
    deaths_in = []
    assist_in = []

    for i in range(tenminutes):
        for j in range(10):
            kills_in.append(' '.join(map(str,kills_list[j][10*i:min(10*(i+1),minutes)])))
            deaths_in.append(' '.join(map(str,deaths_list[j][10*i:min(10*(i+1),minutes)])))
            assist_in.append(' '.join(map(str,assist_list[j][10*i:min(10*(i+1),minutes)])))
            
    time_df.loc[:,'kills'] = kills_in
    time_df.loc[:,'deaths'] = deaths_in
    time_df.loc[:,'assists'] = assist_in
    
    
    return time_df
