from copy import deepcopy

def is_where(coordinate):
    where = 'JUNGLE'
    difference = coordinate['x'] - coordinate['y']
    if coordinate['x'] < 3000 and coordinate['y'] < 3000:     where = 'BLUE HOME'
    elif coordinate['x'] > 12000 and coordinate['y'] > 12000: where = 'RED HOME'
    elif coordinate['y'] > 12000 or coordinate['x'] < 3000:   where = 'TOP'
    elif coordinate['x'] > 12000 or coordinate['y'] < 3000:   where = 'BOTTOM'
    elif -1500 < difference < 1500:                           where = 'MID'
        
    return where


def position_check(spells_list, items_list, frame):
    position = [-1 for _ in range(10)]
    
    blue_is_smite = [row.count(11) for row in spells_list[:5]]
    red_is_smite = [row.count(11) for row in spells_list[5:]]
    if sum(blue_is_smite) != 1 or sum(red_is_smite) != 1:
        return -1
    position[1] = blue_is_smite.index(1)
    position[6] = red_is_smite.index(1)+5
    
    support_item = [3850, 3851, 3853, 3862, 3863, 3864, 3858, 3859, 3860, 3854, 3855, 3857]
    blue_is_support = [sum(item in support_item for item in items) for items in items_list[:5]]
    red_is_support = [sum(item in support_item for item in items) for items in items_list[5:]]
    if sum(blue_is_support) != 1 or sum(red_is_support) != 1:
        return -1
    position[4] = blue_is_support.index(1)
    position[9] = red_is_support.index(1)+5
    
    
    position_determined = [position[i] for i in range(10) if position[i] != -1]
    
    for j in range(2):
        position_left = ['TOP', 'MID', 'BOTTOM']
        position_index = {'TOP': j*5, 'MID': j*5+2, 'BOTTOM':j*5+3}
        for i in range(5):
            userid = frame[0]['participantFrames'][str(j*5+i+1)]['participantId'] - 1
            if userid in position_determined:
                continue
                
            where_list = [is_where(frame[minute]['participantFrames'][str(j*5+i+1)]['position']) for minute in range(1, 15)]
            where = max(set(where_list), key = where_list.count)
            if where not in position_left:
                return -1
            
            position[position_index[where]] = userid
            position_left.remove(where)
    
    return position



def get_match(data_dict, time_dict):
    user = data_dict['participants']
    team = data_dict['teams']
    
    
    gameid = data_dict['gameId'] - 4000000000
    keyy_list = [(gameid - 900000000) * 2] + [(gameid - 900000000) * 2 + 1]
    team_list = [100, 200]
    win_list = [1, 0] if team[0]['win'] == 'Win' else [0, 1]
    creation = data_dict['gameCreation'] // 1000
    duration = data_dict['gameDuration']
    
    picks_list = [user[i]['championId'] for i in range(10)]
    bans_list = [team[0]['bans'][i]['championId'] for i in range(5)] + [team[1]['bans'][i]['championId'] for i in range(5)]
    
    spells_list = [[user[i]['spell1Id'], user[i]['spell2Id']] for i in range(10)]
    
    total_kills_list = [sum([user[i]['stats']['kills'] for i in range(5)]), sum([user[i]['stats']['kills'] for i in range(5,10)])]
    total_deaths_list = [sum([user[i]['stats']['deaths'] for i in range(5)]), sum([user[i]['stats']['deaths'] for i in range(5,10)])]
    total_assists_list = [sum([user[i]['stats']['assists'] for i in range(5)]), sum([user[i]['stats']['assists'] for i in range(5,10)])]
    
    
    try:
        runes_list = [[user[i]['stats']['perk0'], user[i]['stats']['perkSubStyle']] for i in range(10)]
    except:
        perk_to_sub = {}
        perk_list = [[8126, 8139, 8143, 8136, 8120, 8138, 8135, 8134, 8105, 8106], [8306, 8304, 8313, 8321, 8316, 8345, 8347, 8410, 8352],
                     [9101, 9111, 8009, 9104, 9105, 9103, 8014, 8017, 8299], [8446, 8463, 8401, 8429, 8444, 8473, 8451, 8453, 8242],
                     [8224, 8226, 8275, 8210, 8234, 8233, 8237, 8232, 8236]]
        sub_list = [8100, 8300, 8000, 8400, 8200]
        
        for i in range(5):
            for j in range(len(perk_list[i])):
                perk_to_sub[perk_list[i][j]] = sub_list[i]
                
        for i in range(10):
            if 'perkSubStyle' not in user[i]['stats']:
                user[i]['stats']['perkSubStyle'] = perk_to_sub[user[i]['stats']['perk4']]
                
        runes_list = [[user[i]['stats']['perk0'], user[i]['stats']['perkSubStyle']] for i in range(10)]
        
    
    items_list = [[user[i]['stats']['item0'], user[i]['stats']['item1'], user[i]['stats']['item2'], user[i]['stats']['item3'], 
                   user[i]['stats']['item4'], user[i]['stats']['item5'], user[i]['stats']['item6']] for i in range(10)]
    
    p_to_u = position_check(spells_list, items_list, time_dict)
    if p_to_u == -1:
        return -1
    
    f_to_u = [time_dict[0]['participantFrames'][str(i+1)]['participantId']-1 for i in range(10)]
    u_to_f = [str(f_to_u.index(i)+1) for i in range(10)]
    p_to_f = [u_to_f[p_to_u[i]] for i in range(10)]
    
    
    
    kda = [[0, 0, 0] for _ in range(10)]
    kda_minute = [[]]
    for i in range(1, 15):
        event_dict = time_dict[i]['events']
        for event in event_dict:
            if event['type'] == 'CHAMPION_KILL':
                kda[event['killerId']-1][0] += 1
                kda[event['victimId']-1][1] += 1
                for assist in event['assistingParticipantIds']:
                    kda[assist-1][2] += 1
                    
        kda_minute.append(deepcopy(kda))

        
    
        
    summary = [{'keyy'   : keyy_list[i],
                'gameid' : gameid,
                'team'   : team_list[i],
                'win'    : win_list[i],
                'creation' : creation,
                'duration' : duration,
                'top_p' : picks_list[p_to_u[i*5+0]],
                'jgl_p' : picks_list[p_to_u[i*5+1]],
                'mid_p' : picks_list[p_to_u[i*5+2]],
                'bot_p' : picks_list[p_to_u[i*5+3]],
                'sup_p' : picks_list[p_to_u[i*5+4]],
                'top_b' : bans_list[int(p_to_f[i*5+0])-1],
                'jgl_b' : bans_list[int(p_to_f[i*5+1])-1],
                'mid_b' : bans_list[int(p_to_f[i*5+2])-1],
                'bot_b' : bans_list[int(p_to_f[i*5+3])-1],
                'sup_b' : bans_list[int(p_to_f[i*5+4])-1]} for i in range(2)]
               
    
    result_list = [[{'keyy'  : keyy_list[i],
                    'gameid' : gameid,
                    'team'   : team_list[i],
                    'win'    : win_list[i],
                    'duration'  : duration,
                    'champion'  : picks_list[p_to_u[i*5+j]],
                    'spell1'    : spells_list[p_to_u[i*5+j]][0],
                    'spell2'    : spells_list[p_to_u[i*5+j]][1],
                    'rune_main' : runes_list[p_to_u[i*5+j]][0],
                    'rune_sub'  : runes_list[p_to_u[i*5+j]][1],
                    'kills'     : user[p_to_u[i*5+j]]['stats']['kills'],
                    'deaths'    : user[p_to_u[i*5+j]]['stats']['deaths'],
                    'assists'   : user[p_to_u[i*5+j]]['stats']['assists'],
                    'level'     : user[p_to_u[i*5+j]]['stats']['champLevel'],
                    'cs'        : user[p_to_u[i*5+j]]['stats']['totalMinionsKilled'],
                    'gold'      : user[p_to_u[i*5+j]]['stats']['goldEarned'],
                    'item0'     : items_list[p_to_u[i*5+j]][0],
                    'item1'     : items_list[p_to_u[i*5+j]][1],
                    'item2'     : items_list[p_to_u[i*5+j]][2],
                    'item3'     : items_list[p_to_u[i*5+j]][3],
                    'item4'     : items_list[p_to_u[i*5+j]][4],
                    'item5'     : items_list[p_to_u[i*5+j]][5],
                    'item6'     : items_list[p_to_u[i*5+j]][6],
                    'kills_total'   : total_kills_list[i],
                    'deaths_total'  : total_deaths_list[i],
                    'assists_total' : total_assists_list[i]} for i in range(2)] for j in range(5)]
    
    
    detail_list = [[{'keyy'  : keyy_list[i],
                     'gameid' : gameid,
                     'team'   : team_list[i],
                     'win'    : win_list[i],
                     'duration' : duration,
                     'champion' : picks_list[p_to_u[i*5+j]],
                     'deaths'   : user[p_to_u[i*5+j]]['stats']['deaths'],
                     'dmg_dealt_champ'  : user[p_to_u[i*5+j]]['stats']['totalDamageDealtToChampions'], 
                     'dmg_dealt_object' : user[p_to_u[i*5+j]]['stats']['damageDealtToObjectives'],
                     'dmg_dealt_tower'  : user[p_to_u[i*5+j]]['stats']['damageDealtToTurrets'], 
                     'dmg_taken'        : user[p_to_u[i*5+j]]['stats']['totalDamageTaken'],
                     'dmg_mitigated'    : user[p_to_u[i*5+j]]['stats']['damageSelfMitigated'],  
                     'total_heal'       : user[p_to_u[i*5+j]]['stats']['totalHeal'], 
                     'cs_minion'        : user[p_to_u[i*5+j]]['stats']['totalMinionsKilled'], 
                     'cs_jungle'        : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilled'],
                     'cs_jungle_team'   : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilledTeamJungle'], 
                     'cs_jungle_enemy'  : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilledEnemyJungle'], 
                     'vision_score'     : user[p_to_u[i*5+j]]['stats']['visionScore'], 
                     'ward_bought'      : user[p_to_u[i*5+j]]['stats']['visionWardsBoughtInGame'], 
                     'ward_placed'      : user[p_to_u[i*5+j]]['stats']['wardsPlaced'], 
                     'ward_killed'      : user[p_to_u[i*5+j]]['stats']['wardsKilled']} for i in range(2)] for j in range(5)]
    
    
    laning_list = [[{'keyy'   : keyy_list[i],
                     'gameid' : gameid,
                     'team'   : team_list[i],
                     'win'    : win_list[i],
                     'duration' : duration,
                     'champion'  : picks_list[p_to_u[i*5+j]],
                     'kills'     : kda_minute[14][p_to_u[i*5+j]][0],
                     'deaths'    : kda_minute[14][p_to_u[i*5+j]][1],
                     'assists'   : kda_minute[14][p_to_u[i*5+j]][2],
                     'level'     : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['level'],
                     'xp'        : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['xp'],
                     'gold'      : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['totalGold'],
                     'cs_minion' : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['minionsKilled'],
                     'cs_jungle' : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['jungleMinionsKilled']} 
                   for i in range(2)] for j in range(5)]
    
    '''
    minute_list = [[{'keyy'   : keyy_list[0],
                      'gameid' : gameid,
                      'team'   : team_list[0],
                      'win'    : win_list[0],
                      'champion'  : picks_list[p_to_u[j]],
                      'minute'    : minute,
                      'kills'     : kda_minute[minute][p_to_u[j]][0],
                      'deaths'    : kda_minute[minute][p_to_u[j]][1],
                      'assists'   : kda_minute[minute][p_to_u[j]][2],
                      'level'     : time_dict[minute]['participantFrames'][p_to_f[j]]['level'],
                      'xp'        : time_dict[minute]['participantFrames'][p_to_f[j]]['xp'],
                      'gold'      : time_dict[minute]['participantFrames'][p_to_f[j]]['totalGold'],
                      'cs_minion' : time_dict[minute]['participantFrames'][p_to_f[j]]['minionsKilled'],
                      'cs_jungle' : time_dict[minute]['participantFrames'][p_to_f[j]]['jungleMinionsKilled']} 
                   for minute in range(1, len(time_dict))] + 
                   [{'keyy'   : keyy_list[1],
                      'gameid' : gameid,
                      'team'   : team_list[1],
                      'win'    : win_list[1],
                      'champion'  : picks_list[p_to_u[5+j]],
                      'minute'    : minute,
                      'kills'     : kda_minute[minute][p_to_u[5+j]][0],
                      'deaths'    : kda_minute[minute][p_to_u[5+j]][1],
                      'assists'   : kda_minute[minute][p_to_u[5+j]][2],
                      'level'     : time_dict[minute]['participantFrames'][p_to_f[5+j]]['level'],
                      'xp'        : time_dict[minute]['participantFrames'][p_to_f[5+j]]['xp'],
                      'gold'      : time_dict[minute]['participantFrames'][p_to_f[5+j]]['totalGold'],
                      'cs_minion' : time_dict[minute]['participantFrames'][p_to_f[5+j]]['minionsKilled'],
                      'cs_jungle' : time_dict[minute]['participantFrames'][p_to_f[5+j]]['jungleMinionsKilled']} 
                   for minute in range(1, len(time_dict))] for j in range(5)]
    '''
    
    
    return [summary, result_list, detail_list, laning_list]
    #return [summary, result_list, detail_list, laning_list, minute_list]
