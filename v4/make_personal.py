import pandas as pd
from copy import deepcopy

def is_where(position):
    where = ['JUNGLE']*5
    for i in range(5):
        difference = position[i]['x'] - position[i]['y']
        if position[i]['x'] < 3000 and position[i]['y'] < 3000:     where[i] = 'BLUE HOME'
        elif position[i]['x'] > 12000 and position[i]['y'] > 12000: where[i] = 'RED HOME'
        elif position[i]['y'] > 12000 or position[i]['x'] < 3000:   where[i] = 'TOP'
        elif position[i]['x'] > 12000 or position[i]['y'] < 3000:   where[i] = 'BOTTOM'
        elif -1500 < difference < 1500:                             where[i] = 'MID'
        
    return where


def role(items, spell, line_check):
    line_list = ['UNKNOWN' for _ in range(5)]
    
    where_list = []
    for i in range(2, len(line_check)):
        where_list.append(is_where(line_check[i]['position'].tolist()))
    where_list = list(map(list, zip(*where_list)))
    
    most_list = [None for _ in range(5)]
    for i in range(5):
        where = max(set(where_list[i]), key=where_list[i].count)
        most_list[i] = where
        
            
    support_item = set([3850, 3851, 3853, 3862, 3863, 3864, 3854, 3855, 3857, 3858, 3859, 3860])
    is_support_item = [bool(set(item)&support_item) for item in items]

    check_support = False
    if is_support_item.count(True) == 1: line_list[is_support_item.index(True)] = 'SUPPORT'
    else: check_support = True


    is_smite = [False for _ in range(5)]

    for i in range(5):
        if spell[i][0] == 11:   is_smite[i] = True
        elif spell[i][1] == 11: is_smite[i] = True

    if is_smite.count(True) == 1:
        line_list[is_smite.index(True)] = 'JUNGLE'
    else:
        min2 = list(line_check[2]['jungleMinionsKilled'].mask(line_check[2]['jungleMinionsKilled']>0, True).isin([True]))
        if min2.count(True) == 1:
            line_list[min2.index(True)] = 'JUNGLE'
        else:
            jungle_max = max(list(line_check[-1]['jungleMinionsKilled']))
            line_list[list(line_check[-1]['jungleMinionsKilled']).index(jungle_max)] = 'JUNGLE'
            
            
    if check_support == False:
        for i in range(5):
            if line_list[i] == 'UNKNOWN':
                if most_list[i] in ['TOP', 'MID', 'BOTTOM']: line_list[i] = most_list[i]
                else: line_list[i] = 'UNSPECIFIED'
                    
        if line_list.count('UNSPECIFIED') == 1:
            unspecified_index = line_list.index('UNSPECIFIED')
            if 'TOP' not in line_list:   line_list[unspecified_index] = 'TOP'
            elif 'MID' not in line_list: line_list[unspecified_index] = 'MID'
            else:                         line_list[unspecified_index] = 'BOTTOM'
    
    else:
        jungle_index = line_list.index('JUNGLE')
        
        copy_line = [line_list[i] for i in range(5)]
        copy_most = [most_list[i] for i in range(5)]
        
        del(copy_line[jungle_index])
        del(copy_most[jungle_index])
        
        if copy_most.count('TOP') == 1:    copy_line[copy_most.index('TOP')] = 'TOP'
        if copy_most.count('MID') == 1:    copy_line[copy_most.index('MID')] = 'MID'
        if copy_most.count('BOTTOM') == 1: copy_line[copy_most.index('BOTTOM')] = 'BOTTOM'
        
        if copy_line.count('UNKNOWN') == 1:  copy_line[copy_line.index('UNKNOWN')] = 'SUPPORT'
        elif copy_line.count('UNKNOWN') > 2: copy_line = ['UNSPECIFIED' if x=='UNKNOWN' else x for x in copy_line]
        
        copy_line.insert(jungle_index, 'JUNGLE')
        line_list = copy_line
        
        if line_list.count('UNKNOWN') == 2:
            unknown_indices = [index for index, value in enumerate(line_list) if value == 'UNKNOWN']
            
            min10_minion = list(line_check[-1]['minionsKilled'])
            if min10_minion[unknown_indices[0]] == min10_minion[unknown_indices[1]]:
                line_list[unknown_indices[0]] = 'UNSPECIFIED'
                line_list[unknown_indices[1]] = 'UNSPECIFIED'
            elif min10_minion[unknown_indices[0]] > min10_minion[unknown_indices[1]]:
                line_list[unknown_indices[1]] = 'SUPPORT'
                unknown_index = unknown_indices[0]
            
                if line_list.count('TOP') == 0: line_list[unknown_index] = 'TOP'
                if line_list.count('MID') == 0: line_list[unknown_index] = 'MID'
                else:                           line_list[unknown_index] = 'BOTTOM'
 
            else:
                line_list[unknown_indices[0]] = 'SUPPORT'
                unknown_index = unknown_indices[1]
            
                if line_list.count('TOP') == 0: line_list[unknown_index] = 'TOP'
                if line_list.count('MID') == 0: line_list[unknown_index] = 'MID'
                else:                           line_list[unknown_index] = 'BOTTOM'
    
    return line_list



def get_ps(user, stat, frame, nickname):
    summary_columns = ['gameid', 'userid', 'team', 'win', 'nickname', 'role', 'champion', 'level', 'kills', 'deaths', 'assists', 
                       'spells', 'gold', 'items', 'skilltree']
    summary_df = pd.DataFrame(index=[i for i in range(10)], columns=summary_columns)

    summary_df['userid'] = [i for i in range(1, 11)]
    summary_df['team'] = ['BLUE']*5 + ['RED']*5
    
    win_dict = {True:'VICTORY', False:'DEFEAT'}
    team_win = {'BLUE':win_dict[stat['win'][0]], 'RED':win_dict[stat['win'][5]]}
    summary_df['win'] = summary_df['team'].replace(team_win)
    summary_df['nickname'] = nickname
    summary_df['champion'] = user['championId']
    
    spells = user[['spell1Id','spell2Id']].values.tolist()
    summary_df['spells'] = spells
    
    items = stat[['item0','item1','item2','item3','item4','item5','item6']].values.tolist()
    summary_df['items'] = items

    summary_df[['level','kills','deaths','assists','gold']] = stat[['champLevel','kills','deaths','assists','goldEarned']]
    
    frame_blue, frame_red = [], []
    for i in range(len(frame)):
        minute_data = pd.DataFrame(frame.iloc[i].values(), index=frame.iloc[i].keys()).sort_values('participantId')
        frame_blue.append(minute_data[:5])
        frame_red.append(minute_data[5:])
    summary_df['role'] = role(items[:5], spells[:5], frame_blue) + role(items[5:], spells[5:], frame_red)  
    
    
    return summary_df



def get_pr(ingame_rune):
    rune_columns = ['gameid', 'userid', 'runes', 'main', 'sub', 'shard']
    rune_df = pd.DataFrame(index=[i for i in range(10)], columns=rune_columns)
    
    rune_df['userid'] = [i for i in range(1,11)]
    rune_df['runes'] = ingame_rune[['perkPrimaryStyle','perkSubStyle']].values.tolist()
    rune_df['main'] = ingame_rune[['perk0','perk1','perk2','perk3']].values.tolist()
    rune_df['sub'] = ingame_rune[['perk4','perk5']].values.tolist()
    rune_df['shard'] = ingame_rune[['statPerk0','statPerk1','statPerk2']].values.tolist()
    
    
    return rune_df



def get_pd(ingame):
    detail_columns = ['gameid', 'userid', 'damagedealt', 'damagetaken', 'cs', 'visions', 'firsts']
    detail_df = pd.DataFrame(index=[i for i in range(10)], columns=detail_columns)

    detail_df['userid'] = [i for i in range(1, 11)]
    
    damage_dealt = ['totalDamageDealtToChampions','magicDamageDealtToChampions','physicalDamageDealtToChampions', 
                    'trueDamageDealtToChampions','damageDealtToObjectives','damageDealtToTurrets']
    detail_df['damagedealt'] = ingame[damage_dealt].values.tolist()
    
    damage_taken = ['totalDamageTaken','magicalDamageTaken','physicalDamageTaken','trueDamageTaken','totalHeal','damageSelfMitigated']
    detail_df['damagetaken'] = ingame[damage_taken].values.tolist()
    
    cs = ['totalMinionsKilled','neutralMinionsKilled','neutralMinionsKilledTeamJungle','neutralMinionsKilledEnemyJungle']
    detail_df['cs'] = ingame[cs].values.tolist()
    
    vision = ['visionScore','visionWardsBoughtInGame','wardsPlaced','wardsKilled']
    detail_df['visions'] = ingame[vision].values.tolist()
    
    bool_dict = {0:False}
    detail_df['firsts'] = ingame[['firstBloodKill','firstTowerKill']].replace(bool_dict).values.tolist()
    
    
    return detail_df
