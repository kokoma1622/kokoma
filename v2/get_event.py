import pandas as pd

    
def champ_kill(event):  
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    new_event['positionX'] = event['position']['x']
    new_event['positionY'] = event['position']['y']
    new_event['killerId'] = event['killerId']
    new_event['victimId'] = event['victimId']
    new_event['assistIds'] = ' '.join(map(str,event['assistingParticipantIds']))
    
    return new_event
    
def ward(event):
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    new_event['eventType'] = event['type']
    new_event['wardType'] = event['wardType']
    if event['type'] == 'WARD_PLACED':
        new_event['participantId'] = event['creatorId']
    else:
        new_event['participantId'] = event['killerId']
        
    return new_event

def item(event):
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    new_event['eventType'] = event['type']
    new_event['participantId'] = event['participantId']
    
    if event['type'] == 'ITEM_UNDO':
        new_event['itemId'] = event['afterId']
        new_event['fromItemId'] = event['beforeId']
    else:
        new_event['itemId'] = event['itemId']
        
    return new_event

def skillup(event):
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    new_event['participantId'] = event['participantId']
    new_event['skillSlot'] = event['skillSlot']
    new_event['levelupType'] = event['levelUpType']
    
    return new_event

def building(event):
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    if event['killerId'] < 6:
        new_event['teamId'] = 'BLUE'
    else:
        new_event['teamId'] = 'RED'
    new_event['killerId'] = event['killerId']
    new_event['assistIds'] = ' '.join(map(str,event['assistingParticipantIds']))
    new_event['buildingType'] = event['buildingType']
    new_event['laneType'] = event['laneType']
    new_event['towerType'] = event['towerType']
    
    return new_event

def monster(event):
    new_event = {}
    
    new_event['timestamp'] = event['timestamp']
    if event['killerId'] < 6:
        new_event['teamId'] = 'BLUE'
    else:
        new_event['teamId'] = 'RED'
    new_event['killerId'] = event['killerId']
    new_event['monsterType'] = event['monsterType']
    if 'monsterSubType' in event:
        new_event['dragonSubType'] = event['monsterSubType']
    
    return new_event
    
    

def events(event):
    event_list = [[] for _ in range(5)]
    
    for i in range(len(event)):
        for j in range(len(event.iloc[i])):
            one_event = event.iloc[i][j]
            if one_event['type'] == 'CHAMPION_KILL':
                event_list[0].append(champ_kill(one_event))
            #elif one_event['type'] == 'WARD_PLACED' and one_event['wardType'] != 'UNDEFINED':
            #    event_list[1].append(ward(one_event))
            #elif one_event['type'] == 'WARD_KILL':
            #    event_list[1].append(ward(one_event))
            elif one_event['type'] in ['ITEM_PURCHASED', 'ITEM_SOLD']:
                if one_event['itemId'] not in [2003, 2055, 3330, 3340, 3363, 3364]:
                    event_list[1].append(item(one_event))
            elif one_event['type'] == 'ITEM_UNDO':
                event_list[1].append(item(one_event))
            elif one_event['type'] == 'SKILL_LEVEL_UP':
                event_list[2].append(skillup(one_event))
            elif one_event['type'] == 'BUILDING_KILL':
                event_list[3].append(building(one_event))
            elif one_event['type'] == 'ELITE_MONSTER_KILL':
                event_list[4].append(monster(one_event))

    event_df_list = [pd.DataFrame(event_list[i]) for i in range(5)]
                
    return event_df_list
