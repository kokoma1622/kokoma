import pandas as pd
from copy import deepcopy

def get_ms(frame, kda, items, tower, dragon):
    column_p = ['gameid', 'minute', 'kills', 'deaths', 'assists', 'level', 'items', 'gold', 'cs']
    minute_p_df = pd.DataFrame(index=[i for i in range(len(frame)-1)], columns=column_p)
    
    minute_p_df['minute'] = [i for i in range(1,len(frame))]
    minute_p_df['kills'] = kda['kills']
    minute_p_df['deaths'] = kda['deaths']
    minute_p_df['assists'] = kda['assists']
    
    if len(frame)-1 > len(items):
        for _ in range(len(frame)-1-len(items)):
            items.append(items[-1])
    elif len(frame)-1 < len(items):
        items = items[:len(frame)-1]
    minute_p_df['items'] = items
    
    level, gold, cs = [], [], []
    for i in range(len(frame)):
        minute_data = pd.DataFrame(frame.iloc[i].values(), index=frame.iloc[i].keys()).sort_values('participantId')
        level.append(minute_data['level'].values.tolist())
        gold.append(minute_data['totalGold'].values.tolist())
        cs.append((minute_data['minionsKilled']+minute_data['jungleMinionsKilled']).values.tolist())
    minute_p_df['level'] = level[1:]
    minute_p_df['gold'] = gold[1:]
    minute_p_df['cs'] = cs[1:]
    
    
    
    column_t = ['gameid', 'minute', 'golds', 'kills', 'towers', 'dragons']
    minute_t_df = pd.DataFrame(index=[i for i in range(len(frame)-1)], columns=column_t)
    minute_t_df['minute'] = [i for i in range(1,len(frame))]
    
    gold_blue = [sum(golds[:5]) for golds in minute_p_df['gold'].tolist()]
    gold_red = [sum(golds[5:]) for golds in minute_p_df['gold'].tolist()]
    minute_t_df['golds'] = [[gold_blue[i],gold_red[i]] for i in range(len(frame)-1)]
    
    kill_blue = [sum(kills[:5]) for kills in kda['kills']]
    kill_red = [sum(kills[5:]) for kills in kda['kills']]
    minute_t_df['kills'] = [[kill_blue[i],kill_red[i]] for i in range(len(frame)-1)]
    
    towers = [0, 0]
    towers_m = []
    for i in range(len(tower)):
        event = tower.iloc[i]
        if event['building'][0] == 'T':
            time = event['tstamp']//60000 + 1
            if len(towers_m) < time:
                for _ in range(time-1-len(towers_m)):
                    towers_m.append(deepcopy(towers))
            if event['team'] == 'RED': towers[0] += 1
            else:                      towers[1] += 1
    for _ in range(len(frame)-1-len(towers_m)):
        towers_m.append(deepcopy(towers))
    minute_t_df['towers'] = towers_m
    
    dragons = [['_'], ['_']]
    dragons_m = []
    dragon_count, spirit = 0, None
    for i in range(len(dragon)):
        event = dragon.iloc[i]
        if event['monster'][0] == 'D':
            time = event['tstamp']//60000 + 1
            if len(dragons_m) < time:
                for _ in range(time-1-len(dragons_m)):
                    dragons_m.append(deepcopy(dragons))
            if event['team'] == 'BLUE': 
                if dragons[0][0] == '_': dragons[0][0] = event['monster'].split('_')[1]
                else       :             dragons[0].append(event['monster'].split('_')[1])
            else:
                if dragons[1][0] == '_': dragons[1][0] = event['monster'].split('_')[1]
                else:                    dragons[1].append(event['monster'].split('_')[1])
    for _ in range(len(frame)-1-len(dragons_m)):
        dragons_m.append(deepcopy(dragons))
    
    for i in range(len(dragons_m)):
        if len(dragons_m[i][0]) > len(dragons_m[i][1]):
            for _ in range(len(dragons_m[i][0])-len(dragons_m[i][1])):
                dragons_m[i][1].append('_')
        elif len(dragons_m[i][0]) < len(dragons_m[i][1]):
            for _ in range(len(dragons_m[i][1])-len(dragons_m[i][0])):
                dragons_m[i][0].append('_')
                
    minute_t_df['dragons'] = dragons_m
    
    
    return minute_p_df, minute_t_df
