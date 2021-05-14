import pandas as pd
from copy import deepcopy

def get_em(monster_event, champions, monster_info):
    em_df = pd.DataFrame(monster_event)
    ts_rename = {'dragonKills': 'totalDragon', 'baronKills': 'totalBaron', 'riftHeraldKills': 'totalHerald'}
    ts_df = monster_info.rename(columns=ts_rename)
    
    
    count = [{'BLUE':0, 'RED':0} for _ in range(3)]
    count_no0 = [{'BLUE':0, 'RED':0} for _ in range(3)]
    for j in range(len(em_df)):
        if em_df.loc[j,'monster'] == 'HERALD':
            count[1][em_df.loc[j,'team']] += 1
            if em_df.loc[j,'userid'] != 0:
                count_no0[1][em_df.loc[j,'team']] += 1
        elif em_df.loc[j,'monster'] == 'BARON':
            count[2][em_df.loc[j,'team']] += 1
            if em_df.loc[j,'userid'] != 0:
                count_no0[2][em_df.loc[j,'team']] += 1
        else:
            count[0][em_df.loc[j,'team']] += 1
            if em_df.loc[j,'userid'] != 0:
                count_no0[0][em_df.loc[j,'team']] += 1
    
    
    if count[1]['BLUE']+count[1]['RED'] != ts_df.loc[0,'totalHerald']+ts_df.loc[1,'totalHerald']:
        em_df = em_df.drop(em_df[em_df['monster']=='HERALD'].index[-1], axis=0)
        em_df.index = [i for i in range(len(em_df))]
    
    
    herald_gap_blue = ts_df.loc[0,'totalHerald'] - count_no0[1]['BLUE']
    herald_gap_red = ts_df.loc[1,'totalHerald'] - count_no0[1]['RED']
    
    if herald_gap_blue != 0 and herald_gap_red != 0:
        for i in range(len(em_df)):
            if em_df.loc[i,'monster'] == 'HERALD':
                em_df.loc[i,'team'] = 'UNCLEAR'
                em_df.loc[i,'userid'] = -1
    elif herald_gap_blue != 0:
        for i in range(len(em_df)):
            if em_df.loc[i,'monster'] == 'HERALD' and em_df.loc[i,'userid'] == 0:
                em_df.loc[i,'team'] = 'BLUE'
                em_df.loc[i,'userid'] = -1
    elif herald_gap_red != 0:
        for i in range(len(em_df)):
            if em_df.loc[i,'monster'] == 'HERALD' and em_df.loc[i,'userid'] == 0:
                em_df.loc[i,'team'] = 'RED'
                em_df.loc[i,'userid'] = -1
    
    
    blue_champ, red_champ = champions[:5], champions[5:]
    blue_shaco, red_shaco = ('Shaco' in blue_champ), ('Shaco' in red_champ)  
    

    if blue_shaco:
        red_sylas = ('Sylas' in red_champ)
        if not red_sylas:
            for j in range(len(em_df)):
                if em_df.loc[j,'userid'] == 0:
                    em_df.loc[j,'userid'] = blue_champ.index('Shaco')+1  
        else:
            dragon_gap = ts_df.loc[1,'totalDragon'] - count_no0[0]['RED']
            herald_gap = ts_df.loc[1,'totalHerald'] - count_no0[1]['RED']
            nashor_gap = ts_df.loc[1,'totalBaron'] - count_no0[2]['RED']

            if dragon_gap != 0:
                if ts_df.loc[0,'totalDragon'] - count_no0[0]['BLUE'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'] == 'DRAGON':
                            em_df.loc[j,'team'] = 'RED'
                            em_df.loc[j,'userid'] = red_champ.index('Sylas')+6
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'] == 'DRAGON':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1
            if herald_gap != 0:
                if ts_df.loc[0,'totalHerald'] - count_no0[1]['BLUE'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'] == 'HERALD':
                            em_df.loc[j,'team'] = 'RED'
                            em_df.loc[j,'userid'] = red_champ.index('Sylas')+6
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'] == 'HERALD':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1
            if nashor_gap != 0:
                if ts_df.loc[0,'totalBaron'] - count_no0[2]['BLUE'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'] == 'BARON':
                            em_df.loc[j,'team'] = 'RED'
                            em_df.loc[j,'userid'] = red_champ.index('Sylas')+6
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'] == 'BARON':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1

            for j in range(len(em_df)):
                if em_df.loc[j,'userid'] == 0:
                    em_df.loc[j,'userid'] = blue_champ.index('Shaco')+1  

    elif red_shaco:
        blue_sylas = ('Syals' in blue_champ)
        if not blue_sylas:
            for j in range(len(em_df)):
                if em_df.loc[j,'userid'] == 0:
                    em_df.loc[j,'team'] = 'RED'
                    em_df.loc[j,'userid'] = red_champ.index('Shaco')+6
        else:
            dragon_gap = ts_df.loc[0,'totalDragon'] - count_no0[0]['BLUE']
            herald_gap = ts_df.loc[0,'totalHerald'] - count_no0[1]['BLUE']
            nashor_gap = ts_df.loc[0,'totalBaron'] - count_no0[2]['BLUE']

            if dragon_gap != 0:
                if ts_df.loc[1,'totalDragon'] - count_no0[0]['RED'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'][0] == 'D':
                            em_df.loc[j,'team'] = 'BLUE'
                            em_df.loc[j,'userid'] = blue_champ.index('Sylas')+1
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'][0] == 'D':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1
            if herald_gap != 0:
                if ts_df.loc[1,'totalHerald'] - count_no0[1]['RED'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'] == 'HERALD':
                            em_df.loc[j,'team'] = 'BLUE'
                            em_df.loc[j,'userid'] = blue_champ.index('Sylas')+1
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'] == 'HERALD':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1
            if nashor_gap != 0:
                if ts_df.loc[1,'totalBaron'] - count_no0[2]['RED'] == 0:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'userid'] == 0 and em_df.loc[j,'monster'] == 'BARON':
                            em_df.loc[j,'team'] = 'BLUE'
                            em_df.loc[j,'userid'] = blue_champ.index('Sylas')+1
                else:
                    for j in range(len(em_df)):
                        if em_df.loc[j,'monster'] == 'BARON':
                            em_df.loc[j,'team'] = 'UNCLEAR'
                            em_df.loc[j,'userid'] = -1

            for j in range(len(em_df)):
                if em_df.loc[j,'userid'] == 0:
                    em_df.loc[j,'userid'] = red_champ.index('Shaco')+6 
    
    

    return em_df
