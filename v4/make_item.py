import pandas as pd
from copy import deepcopy


def item_df(event, event_type):
    return {'gameid' : 0,
             'tstamp' : event['timestamp'],
             'userid' : event['participantId'],
             'action' : event_type,
             'itemid' : event['itemId']
            }

def make_items_list(items):
    items_list = []
    
    consumable = [2003, 2055, 2010, 2403]
    rune_item = [2010, 2422]
    error, error_mem = 0, -1
    
    for minute in items:
        items_minute = []
        
        for i in range(10):
            member = minute[i]
            items_had = []
            trinket = member['trinket']
            now_items = list(member.keys())
            del now_items[:2]
            
            if member['count'] > 6:
                for _ in range(member['count']-6):
                    if now_items[-1] not in rune_item:
                        error, error_mem = 1, i
                    now_items.pop()
                    
            for itemid in now_items:
                if itemid not in consumable:
                    for _ in range(member[itemid]):
                        items_had.append(itemid)
                else:
                    items_had.append(itemid)
                    
            for _ in range(max(0,6-member['count'])):
                items_had.append(0)
                
            items_had.append(trinket)
            
            items_minute.append(items_had)
            
        items_list.append(items_minute)
    
    
    if error == 1:
        for i in range(len(items_list)):
            print('%2d'%i, items[i][error_mem])
            
            
    return items_list, error_mem


def get_ei(items, rune_item, shoes, champions):
    items_minute = []
    items_list = [{'count':0, 'trinket':3340 if champions[i]!='Fiddlesticks' else 3330} for i in range(10)]
    event_item = []
    
    trinket = [3340, 3363, 3364, 3330]
    item_rune = [2010, 2422]
    stopwatch = {2419:2423, 2423:2424, 2420:2421}
    stopwatch_use = [0 for _ in range(10)]
    support = {3850:3851, 3851:3853, 3854:3855, 3855:3857, 3858:3859, 3859:3860, 3862:3863, 3863:3864}
    tear = {3003:3040, 3004:3042}
    gankplank = [3901, 3902, 3903]
    consumable = [2003, 2055, 2010, 2403]
    upper_watch = [3026,3157,3193]
    
    
    #item_return = pd.DataFrame(items)
    #print(item_return[item_return['participantId']==5])
    
    
    biscuit = {}
    for i in range(10):
        if rune_item[0][i] == 1:
            items_list[i][2403] = 3
            items_list[i]['count'] += 1
        if rune_item[1][i] == 1:
            items_list[i][2419] = 1
            items_list[i]['count'] += 1
        if rune_item[2][i] == 1:
            biscuit[i] = 1
    biscuit_check = 3
    
    if 112 in champions:
        vik = champions.index(112)
        items_list[vik][3200] = 1
        items_list[vik]['count'] += 1
        
        
    for i in range(len(items)):
        user, itemid, now = items[i]['participantId']-1, items[i]['itemId'], items[i]['timestamp']
        
        if now//60000 > len(items_minute):
            for _ in range(now//60000-len(items_minute)):
                items_minute.append(deepcopy(items_list))
            
            
        if now//60000+1 > 2 and biscuit_check == 3:
            for b_member in biscuit:
                items_list[b_member][2010] = 1
                items_list[b_member]['count'] += 1
            biscuit_check -= 1
                
        if now//60000+1 > 4 and biscuit_check == 2:
            for b_member in biscuit:
                if 2010 in items_list[b_member]:
                    items_list[b_member][2010] += 1
                else:
                    items_list[b_member][2010] = 1
                    items_list[b_member]['count'] += 1
            biscuit_check -= 1
                
        if now//60000+1 > 6 and biscuit_check == 1:
            for b_member in biscuit:
                if 2010 in items_list[b_member]:
                    items_list[b_member][2010] += 1
                else:
                    items_list[b_member][2010] = 1
                    items_list[b_member]['count'] += 1
            biscuit_check -= 1
                
                
        for s_member in list(shoes.keys()):
            if now > shoes[s_member]:
                items_list[s_member][2422] = 1
                items_list[s_member]['count'] += 1
                del shoes[s_member]
                
        #print(items_list[3], items_list[6])
        #if items[i]['participantId']==1: print(items[i])
        if items[i]['type'] == 'ITEM_PURCHASED':
            if itemid not in consumable:
                if itemid in gankplank: event_item.append(item_df(items[i], 'GANGPLANK'))
                else:                   event_item.append(item_df(items[i], 'PURCHASE'))
                    
            if itemid in gankplank:
                continue
            elif itemid in trinket:
                items_list[user]['trinket'] = itemid
            else:
                if itemid not in items_list[user]:
                    items_list[user][itemid] = 1
                    items_list[user]['count'] += 1
                else:
                    items_list[user][itemid] += 1
                    if itemid not in consumable:
                        items_list[user]['count'] += 1

        elif items[i]['type'] == 'ITEM_SOLD':
            if itemid not in consumable:
                event_item.append(item_df(items[i], 'SELL'))
                
            if itemid in trinket:
                items_list[user]['trinket'] = 0
            elif itemid not in items_list[user]:
                print('sold not in bug', items[i])
                
            elif items_list[user][itemid] == 1:
                del items_list[user][itemid]
                items_list[user]['count'] -= 1
            else:
                items_list[user][itemid] -= 1
                if itemid not in consumable:
                    items_list[user]['count'] -= 1

        elif items[i]['type'] == 'ITEM_DESTROYED':
            #print(items[i])
            if itemid == 3513:
                event_item.append(item_df(items[i], 'HERALD'))
            elif itemid == 3400:
                event_item.append(item_df(items[i], 'YOURCUT'))
            elif itemid in gankplank:
                continue
            elif itemid in trinket:
                continue
            elif itemid in stopwatch:
                #print(items[i])
                upgrade = 0
                for j in range(i-1,-1,-1):
                    if items[j]['timestamp'] == now and items[j]['participantId']-1 == user:
                        if items[j]['type'] == 'ITEM_DESTROYED':
                            pass
                        elif items[j]['type'] == 'ITEM_PURCHASED' and items[j]['itemId'] in upper_watch:
                            upgrade = 1
                            break
                        elif items[j]['type'] == 'ITEM_PURCHASED' or items[j]['type'] == 'ITEM_SOLD':
                            pass
                        else: 
                            break
                    elif items[j]['timestamp'] == now and items[j]['participantId']-1 != user:
                        pass
                    else: 
                        break
                
                if upgrade == 1:
                    if items_list[user][itemid] == 1:
                        del items_list[user][itemid]
                        items_list[user]['count'] -= 1
                    else:
                        items_list[user][itemid] -= 1
                        items_list[user]['count'] -= 1
                else:
                    if itemid == 2419:
                        event_item.append(item_df(items[i], 'WATCH_KIT'))
                        del items_list[user][itemid]
                        if stopwatch_use[user] == 0: items_list[user][2423] = 1
                        else:                        items_list[user][2424] = 1
                    else:
                        event_item.append(item_df(items[i], 'WATCH_USE'))
                        stopwatch_use[user] = 1
                        if items_list[user][itemid] == 1:
                            del items_list[user][itemid]
                            if stopwatch[itemid] not in items_list[user]:
                                items_list[user][stopwatch[itemid]] = 1
                            else:
                                items_list[user][stopwatch[itemid]] += 1
                        else:
                            items_list[user][itemid] -= 1
                            if stopwatch[itemid] not in items_list[user]:
                                items_list[user][stopwatch[itemid]] = 1
                            else:
                                items_list[user][stopwatch[itemid]] += 1
                      
            elif itemid in support:
                event_item.append(item_df(items[i], 'SUP_QUEST'))
                del items_list[user][itemid]
                items_list[user][support[itemid]] = 1
                
            elif itemid in tear:
                if itemid == 3003: event_item.append(item_df(items[i], 'SERAPH'))
                else:              event_item.append(item_df(items[i], 'MURAMANA'))
                del items_list[user][itemid]
                items_list[user][tear[itemid]] = 1
            
            elif itemid not in items_list[user]:
                print('destroy not in bug', items[i])
                
            elif items_list[user][itemid] == 1:
                del items_list[user][itemid]
                items_list[user]['count'] -= 1
            else:
                items_list[user][itemid] -= 1
                if itemid not in consumable:
                    items_list[user]['count'] -= 1

                    
        if items_list[user]['count'] >= 6:
            temp_items = []
            for items_have in list(items_list[user].keys()):
                if items_have in item_rune:
                    temp_items.append([items_have,items_list[user][items_have]])
                    del items_list[user][items_have]
            for temp in temp_items:
                items_list[user][temp[0]] = temp[1]            
    
    
    
    items_minute.append(deepcopy(items_list))
    
    #for i in range(len(items_minute)):
    #    print('%2d'%i, items_minute[i][6])
    
    made_items_list, error_mem = make_items_list(items_minute)
    
    
    
    if error_mem != -1:
        item_return = pd.DataFrame(items)
        print(item_return[item_return['participantId']==error_mem+1])
        
    return pd.DataFrame(event_item), made_items_list, error_mem
