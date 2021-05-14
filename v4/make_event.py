import pandas as pd
from copy import deepcopy

from make_monster import get_em
from make_item import get_ei
from undo_check import get_previous


def champ_kill(event):  
    return {'gameid' : 0,
             'tstamp' : event['timestamp'],
             'userid' : event['killerId'],
             'victim' : event['victimId'],
             'assist' : event['assistingParticipantIds'],
             'position' : [event['position']['x'], event['position']['y']]
            }

def building(event):
    return {'gameid' : 0,
             'tstamp' : event['timestamp'],
             'userid' : event['killerId'],
             'team' : 'BLUE' if event['teamId']==100 else 'RED',
             'lane' : event['laneType'].split('_')[0],
             'building' : 'INHIBITOR' if event['buildingType'][0]=='I' else 'TURRET_'+event['towerType'].split('_')[0]
            }

def monster(event):
    if event['monsterType'] == 'RIFTHERALD':     monster = 'HERALD'
    elif event['monsterType'] == 'BARON_NASHOR': monster = 'BARON'
    else: monster = event['monsterSubType'].split('_')[1]+'_'+event['monsterSubType'].split('_')[0]
        
    return {'gameid' : 0,
             'tstamp' : event['timestamp'],
             'userid' : event['killerId'],
             'team' : 'RED' if event['killerId'] > 5 else 'BLUE',
             'monster' : monster
            }


def get_events(event, champions, monster_info, rune_item):
    kill_event = []
    building_event = []
    monster_event = []
    item_event = []
    skill_event = []
    
    kill_list, death_list, assist_list = [0 for _ in range(10)], [0 for _ in range(10)], [0 for _ in range(10)]
    kda = {'kills':[], 'deaths':[], 'assists':[]}
    item_minute = [[{'count':0} for _ in range(10)]]
    
    shoes = {}
    for i in range(10):
        if rune_item[3][i] == 1: shoes[i] = 12*60000

    trinket = [3340, 3363, 3364, 3330]
    
    for i in range(1,len(event)):
        for one in event[i]:
            if one['type'] == 'CHAMPION_KILL':
                kill_event.append(champ_kill(one))
                
                kill_list[one['killerId']-1] += 1
                if one['killerId']-1 in shoes:
                    now_shoes = shoes[one['killerId']-1]
                    if one['timestamp'] > now_shoes:            pass
                    elif one['timestamp'] < now_shoes - 45000:  shoes[one['killerId']-1] = now_shoes - 45000
                    else:                                       shoes[one['killerId']-1] = one['timestamp']
                
                death_list[one['victimId']-1] += 1
                
                for assist in one['assistingParticipantIds']:
                    assist_list[assist-1] += 1
                    if assist-1 in shoes:
                        now_shoes = shoes[assist-1]
                        if one['timestamp'] > now_shoes:            pass
                        elif one['timestamp'] < now_shoes - 45000:  shoes[assist-1] = now_shoes - 45000
                        else:                                       shoes[assist-1] = one['timestamp']
                        
            elif one['type'] == 'BUILDING_KILL':
                building_event.append(building(one))
                
            elif one['type'] == 'ELITE_MONSTER_KILL':
                monster_event.append(monster(one))
                
            elif one['type'] == 'SKILL_LEVEL_UP':
                if one['levelUpType'] == 'NORMAL':
                    skill_event.append(one)
            
            elif one['type'] in ['ITEM_PURCHASED', 'ITEM_SOLD', 'ITEM_DESTROYED']:
                #print(one)
                #if one['participantId'] == 2: print(one)
                    
                if one['itemId'] in [3599, 3600]: continue
                item_event.append(one)
                
            elif one['type'] == 'ITEM_UNDO':
                #print(one)
                #if one['participantId'] == 2: print(one)
                    
                if one['beforeId'] in [3599, 3600] or one['afterId'] in [3599, 3600]: continue
                
                undo_user, undo_check = one['participantId'], 0
                for j in range(len(item_event)-1, -1, -1):
                    if item_event[j]['participantId'] == undo_user:
                        if item_event[j]['timestamp'] != one['timestamp']:
                            break
                        elif item_event[j]['itemId'] == one['afterId'] or item_event[j]['itemId'] == one['beforeId']:
                            undo_check, undo_mode, undo_start, undo_end = 1, 0, len(item_event)-1, j
                            break
                if j == 0:
                    del item_event[j]
                    continue
                
                if undo_check == 0:
                    undo_start, undo_end = j, j
                    undo_time = item_event[undo_start]['timestamp']
                    #print(item_event[j], 'start')
                    undo_mode = 0
                    for k in range(undo_start, -1, -1):
                        #print(item_event[k], 'check')
                        if item_event[k]['timestamp'] != undo_time:
                            break
                        if item_event[k]['participantId'] != undo_user:
                            continue

                        undo_end = k
                        if k == 0:
                            break


                        if item_event[k]['type'] == 'ITEM_SOLD' \
                                or (item_event[k]['type']=='ITEM_DESTROYED' and item_event[k]['itemId'] in [2003, 2055, 2010, 2403]):
                            if item_event[k-1]['timestamp'] == undo_time and item_event[k-1]['participantId'] == undo_user:
                                if item_event[k-1]['type'] == 'ITEM_SOLD':
                                    undo_mode = 1
                                    break
                                elif item_event[k-1]['type'] == 'ITEM_PURCHASED':
                                    undo_mode = 2
                                    break
                                else:
                                    break
                            else:
                                break

                        elif item_event[k]['type'] == 'ITEM_PURCHASED':
                            #print(item_event[k-1])
                            if item_event[k-1]['timestamp'] == undo_time and item_event[k-1]['participantId'] == undo_user \
                                    and item_event[k-1]['type'] == 'ITEM_PURCHASED':    
                                undo_mode = 3
                                break
                            elif item_event[k-1]['timestamp'] == undo_time and item_event[k-1]['participantId'] == undo_user \
                                    and item_event[k-1]['type'] == 'ITEM_SOLD':
                                undo_mode = 2
                                break
                            else:
                                break
                            
                #print(item_event[undo_end], 'end')
                
                if undo_mode == 0:
                    for j in range(undo_start, undo_end, -1):
                        if item_event[j]['participantId']==one['participantId'] and item_event[j]['type']=='ITEM_DESTROYED':
                            if item_event[j]['itemId'] not in [2419, 3850, 3851, 3854, 3855, 3858, 3859, 3862, 3863]:
                                #if one['participantId']==7: print(item_event[j])
                                del item_event[j]
                    #if one['participantId']==7: print(item_event[undo_end])
                    del item_event[undo_end]
                    
                elif undo_mode == 1:
                    #print(item_event[undo_end]['itemId'], item_event[undo_end-1]['itemId'])
                    if item_event[undo_end]['itemId']==one['afterId'] or item_event[undo_end]['itemId']==one['beforeId']:
                        #print(item_event[undo_end])
                        del item_event[undo_end]
                    else:
                        #print(item_event[undo_end-1])
                        del item_event[undo_end-1]
                        
                elif undo_mode == 2:
                    if item_event[undo_end]['type']=='ITEM_SOLD' and item_event[undo_end]['itemId']==one['afterId']:
                        #print(item_event[undo_end])
                        del item_event[undo_end]
                    elif item_event[undo_end-1]['type']=='ITEM_SOLD' and item_event[undo_end-1]['itemId']==one['afterId']:
                        #print(item_event[undo_end-1])
                        del item_event[undo_end-1]
                    
                    else:
                        for j in range(undo_start, undo_end, -1):
                            if item_event[j]['participantId']==one['participantId'] and item_event[j]['type']=='ITEM_DESTROYED':
                                if item_event[j]['itemId'] not in [2419, 3850, 3851, 3854, 3855, 3858, 3859, 3862, 3863]:
                                    #print(item_event[j])
                                    del item_event[j]
                        if item_event[undo_end]['type']=='ITEM_PURCHASED' and item_event[undo_end]['itemId']==one['beforeId']:
                            del item_event[undo_end]
                        else:
                            del item_event[undo_end-1]
                    
                    '''
                    if item_event[j]['type']=='ITEM_SOLD' and item_event[j]['itemId']==one['afterId']:
                        print(item_event[j])
                        del item_event[j]
                    elif item_event[j]['type']=='ITEM_PURCHASED' and item_event[j]['itemId']==one['beforeId']:
                        print(item_event[j])
                        del item_event[j]
                    else:
                        print(item_event[j-1])
                        del item_event[j-1]
                    '''
                
                    
                else:
                    #print(one)
                    #print(item_event[undo_end])
                    #print(item_event[undo_end-1])
                    if item_event[undo_end]['itemId'] == one['afterId'] or item_event[undo_end]['itemId'] == one['beforeId']:
                        previous_list = get_previous(item_event[undo_end-1], deepcopy(item_event[undo_end+1:undo_start+1]))
                        for j in range(undo_start, undo_end, -1):
                            if j-undo_end-1 not in previous_list:
                                #print(item_event[j])
                                del item_event[j]
                        del item_event[undo_end]
                    else:
                        previous(item_event[undo_end], deepcopy(item_event[undo_end+1:undo_start+1]))
                        for j in range(undo_start, undo_end, -1):
                            if j-undo_end-1 not in previous_list:
                                del item_event[j]
                        del item_event[undo_end-1]
                
                """
                        if item_event[j]['type'] in ['ITEM_PURCHASED','ITEM_SOLD']:
                            if item_event[j]['itemId']==one['beforeId'] or item_event[j]['itemId']==one['afterId']:
                                if j > 0:
                                    if item_event[j]['participantId']==item_event[j-1]['participantId'] and \
                                       item_event[j]['timestamp']==item_event[j-1]['timestamp']:
                                        duplicate = 1
                                        if item_event[j]['type'] == 'ITEM_PURCHASED' or item_event[j-1]['type'] == 'ITEM_PURCHASED':
                                            print(one)
                                            print(item_event[j], j)
                                            print(item_event[j-1], j-1)
                                            
                                break
                        elif item_event[j]['type']=='ITEM_DESTROYED' and item_event[j]['itemId'] in [2003, 2055, 2010, 2403]:
                            break
                if duplicate == 0:        
                    for k in range(len(item_event)-1, j, -1):
                        if item_event[k]['participantId']==one['participantId'] and item_event[k]['type']=='ITEM_DESTROYED':
                            if item_event[k]['itemId'] not in [2419, 3850, 3851, 3854, 3855, 3858, 3859, 3862, 3863]:
                                del item_event[k]
                    del item_event[j]
                else:
                    if item_event[j]['type'] == item_event[j-1]['type']:
                        del item_event[j]
                    else:
                        if item_event[j]['type']=='ITEM_SOLD' and item_event[j]['itemId']==one['afterId']:
                            del item_event[j]
                        elif item_event[j]['type']=='ITEM_PURCHASED' and item_event[j]['itemId']==one['beforeId']:
                            del item_event[j]
                        else:
                            del item_event[j-1]
                """      
                    
                
        kda['kills'].append(deepcopy(kill_list))
        kda['deaths'].append(deepcopy(death_list))
        kda['assists'].append(deepcopy(assist_list))
             
            
    to_qwer = {1:'Q', 2:'W', 3:'E', 4:'R'}
    skilltree_list = ['' for _ in range(10)]
    for skill in skill_event:
        skilltree_list[skill['participantId']-1] += to_qwer[skill['skillSlot']]
        
        
    event_df_list = [pd.DataFrame(kill_event), pd.DataFrame(building_event)]
    event_df_list.append(get_em(monster_event, champions, monster_info))
    item_df_list, items_list, error_mem = get_ei(item_event, rune_item[:3], shoes, champions)
    event_df_list.append(item_df_list)
    
    
    return event_df_list, kda, items_list, skilltree_list
