import pandas as pd


def is_where(position):
    where = ['JUNGLE']*5
    for i in range(5):
        difference = position.iloc[i]['x'] - position.iloc[i]['y']
        if position.iloc[i]['x'] < 3000 and position.iloc[i]['y'] < 3000:
            where[i] = 'BLUE HOME'
        elif position.iloc[i]['x'] > 12000 and position.iloc[i]['y'] > 12000:
            where[i] = 'RED HOME'
        elif position.iloc[i]['y'] > 12000 or position.iloc[i]['x'] < 3000:
            where[i] = 'TOP'
        elif position.iloc[i]['x'] > 12000 or position.iloc[i]['y'] < 3000:
            where[i] = 'BOTTOM'
        elif difference < 1500 and difference > -1500:
            where[i] = 'MID'
        
    return where


def role(item, spell, line_check):
    line_list = ['UNKNOWN' for _ in range(5)]
    
    where_list = [[] for _ in range(5)]
    id_list = list(line_check[0]['participantId'])
    if id_list[0] < 6:
        for i in range(2, min(len(line_check)-1,11)):
            where = is_where(line_check[i]['position'])
            for j in range(5):
                where_list[id_list[j]-1].append(where[j])
    else:
        for i in range(2, min(len(line_check)-1,11)):
            where = is_where(line_check[i]['position'])
            for j in range(5):
                where_list[id_list[j]-6].append(where[j])
    
    
    most_list = [None for _ in range(5)]        
    for i in range(5):
        role = max(set(where_list[i]), key=where_list[i].count)
        most_list[i] = role
        
            
    support_item = [3850, 3851, 3853, 3862, 3863, 3864, 3854, 3855, 3857, 3858, 3859, 3860]    
    is_support_item = [False for _ in range(5)]

    for i in range(5):
        for j in range(7):
            if item.iloc[i,j] in support_item:
                is_support_item[i] = True    

    check_support = False
    if is_support_item.count(True) == 1:
        line_list[is_support_item.index(True)] = 'SUPPORT'
    else:
        check_support = True


    is_smite = [False for _ in range(5)]

    for i in range(5):
        if spell.iloc[i,0] == 'Smite':
            is_smite[i] = True
        elif spell.iloc[i,1] == 'Smite':
            is_smite[i] = True

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
                if most_list[i] in ['TOP', 'MID', 'BOTTOM']:
                    line_list[i] = most_list[i]
                else:
                    line_list[i] = 'UNSPECIFIED'
                    
        if line_list.count('UNSPECIFIED') == 1:
            unspecified_index = line_list.index('UNSPECIFIED')
            if 'TOP' not in line_list:
                line_list[unspecified_index] = 'TOP'
            elif 'MID' not in line_list:
                line_list[unspecified_index] = 'MID'
            else:
                line_list[unspecified_index] = 'BOTTOM'
    
    else:
        jungle_index = line_list.index('JUNGLE')
        
        copy_line = [line_list[i] for i in range(5)]
        copy_most = [most_list[i] for i in range(5)]
        
        del(copy_line[jungle_index])
        del(copy_most[jungle_index])
        
        if copy_most.count('TOP') == 1:
            copy_line[copy_most.index('TOP')] = 'TOP'
        if copy_most.count('MID') == 1:
            copy_line[copy_most.index('MID')] = 'MID'
        if copy_most.count('BOTTOM') == 1:
            copy_line[copy_most.index('BOTTOM')] = 'BOTTOM'
        
        if copy_line.count('UNKNOWN') == 1:
            copy_line[copy_line.index('UNKNOWN')] = 'SUPPORT'
        elif copy_line.count('UNKNOWN') > 2:
            copy_line = ['UNSPECIFIED' if x=='UNKNOWN' else x for x in copy_line]
        
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
            
                if line_list.count('TOP') == 0:
                    line_list[unknown_index] = 'TOP'
                if line_list.count('MID') == 0:
                    line_list[unknown_index] = 'MID'
                else:
                    line_list[unknown_index] = 'BOTTOM'
 
            else:
                line_list[unknown_indices[0]] = 'SUPPORT'
                unknown_index = unknown_indices[1]
            
                if line_list.count('TOP') == 0:
                    line_list[unknown_index] = 'TOP'
                if line_list.count('MID') == 0:
                    line_list[unknown_index] = 'MID'
                else:
                    line_list[unknown_index] = 'BOTTOM'
    
    
    return line_list



def personal(match, frame):
    summary_columns = ['gameId', 'participantId', 'teamId', 'summonerName', 'role', 'win', 'champion', 'level', 
                       'kills', 'deaths', 'assists', 'spell1', 'spell2', 'goldEarned', 'goldSpent', 
                       'item0', 'item1', 'item2', 'item3', 'item4', 'item5', 'item6']
    rune_columns = ['gameId', 'participantId', 'primaryRuneId', 'primaryRune0', 'primaryRune1', 'primaryRune2', 'primaryRune3',
                    'subRuneId', 'subRune0', 'subRune1', 'statRune0', 'statRune1', 'statRune2']
    detail_columns = ['gameId', 'participantId', 'sDamageDealt', 'mDamageDealt', 'pDamageDealt', 'tDamageDealt', 'sDamageTaken',
                      'mDamageTaken', 'pDamageTaken', 'tDamageTaken', 'totalHeal', 'damageSelfMitigated', 'objectDamage', 'turretDamage',
                      'minionCS', 'monsterCS', 'teamMonsterCS', 'enemyMonsterCS', 'visionScore', 'visionWardsBought', 
                      'wardsPlaced', 'wardsKilled', 'turretKills', 'inhibitorKills', 'firstBloodKill', 'firstTowerKill']

    index = [i for i in range(10)]

    summary_df = pd.DataFrame(index=index, columns=summary_columns)
    rune_df = pd.DataFrame(index=index, columns=rune_columns)
    detail_df = pd.DataFrame(index=index, columns=detail_columns)

    gameId = match['gameId'].item()
    summary_df['gameId'] = gameId
    rune_df['gameId'] = gameId
    detail_df['gameId'] = gameId


    participantIdentities = pd.DataFrame(match['participantIdentities']).iloc[0][0]
    for i in range(len(participantIdentities)):
        participantId = participantIdentities[i]['participantId']
        summary_df.loc[participantId-1, 'participantId'] = participantId
        summary_df.loc[participantId-1, 'summonerName'] = participantIdentities[i]['player']['summonerName']

        rune_df.loc[participantId-1, 'participantId'] = participantId
        detail_df.loc[participantId-1, 'participantId'] = participantId


    champ_df = pd.read_csv('../data_csv/champ.csv')
    champ_dict = dict(zip(champ_df['championId'], champ_df['champion']))
    team_dict = {100:'BLUE', 200:'RED'}
    spell_df = pd.read_csv('../data_csv/spell.csv')
    spell_dict = dict(zip(spell_df['spellId'], spell_df['spellName']))

    participants = pd.DataFrame(pd.DataFrame(match['participants']).iloc[0][0])
    for i in range(len(participants)):
        participantId = participants.iloc[i]['participantId']
        summary_df.loc[participantId-1, 'champion'] = champ_dict[participants.iloc[i]['championId']]
        summary_df.loc[participantId-1, 'teamId'] = team_dict[participants.iloc[i]['teamId']]
        summary_df.loc[participantId-1, 'spell1'] = spell_dict[participants.iloc[i]['spell1Id']]
        summary_df.loc[participantId-1, 'spell2'] = spell_dict[participants.iloc[i]['spell2Id']]


    rename_list = {'totalDamageDealtToChampions':'sDamageDealt', 'magicDamageDealtToChampions':'mDamageDealt',
                   'physicalDamageDealtToChampions':'pDamageDealt', 'trueDamageDealtToChampions':'tDamageDealt',
                   'damageDealtToObjectives':'objectDamage', 'damageDealtToTurrets':'turretDamage', 'totalDamageTaken':'sDamageTaken',
                   'magicalDamageTaken':'mDamageTaken', 'physicalDamageTaken':'pDamageTaken', 'trueDamageTaken':'tDamageTaken',
                   'totalMinionsKilled':'minionCS', 'neutralMinionsKilled':'monsterCS', 
                   'neutralMinionsKilledTeamJungle':'teamMonsterCS', 'neutralMinionsKilledEnemyJungle': 'enemyMonsterCS',
                   'visionWardsBoughtInGame':'visionWardsBought', 'champLevel': 'level', 
                   'perkPrimaryStyle': 'primaryRuneId' , 'perk0': 'primaryRune0', 'perk1': 'primaryRune1', 'perk2': 'primaryRune2', 
                   'perk3': 'primaryRune3', 'perkSubStyle': 'subRuneId', 'perk4': 'subRune0', 'perk5': 'subRune1', 
                   'statPerk0': 'statRune0', 'statPerk1': 'statRune1', 'statPerk2': 'statRune2'}

    stat_original = pd.DataFrame(dict(pd.DataFrame(match['participants'].iloc[0])['stats'])).T
    stat_rename =  stat_original.rename(columns=rename_list)
    stat = stat_rename.sort_values(by='participantId')
    stat = stat.fillna(0)
    

    for_summary_list = ['win', 'level', 'item0', 'item1', 'item2', 'item3', 'item4', 'item5', 'item6', 
                        'kills', 'deaths', 'assists', 'goldEarned', 'goldSpent']
    for_rune_list = ['primaryRuneId', 'primaryRune0', 'primaryRune1', 'primaryRune2', 'primaryRune3', 'subRuneId', 
                     'subRune0', 'subRune1', 'statRune0', 'statRune1', 'statRune2']

    rune_table = pd.read_csv('../data_csv/rune.csv')
    rune_dict = dict(zip(rune_table['runeId'], rune_table['runeName']))
    rune_dict[0] = 'UNKNOWN'
    for i in range(10):
        for runeId in for_rune_list:
            if runeId not in stat:
                stat.loc[:,runeId] = 0
            stat.loc[i,runeId] = rune_dict[stat.loc[i,runeId]]

    for_detail_list = ['sDamageDealt', 'mDamageDealt', 'pDamageDealt', 'tDamageDealt', 'sDamageTaken', 'mDamageTaken', 
                       'pDamageTaken', 'tDamageTaken', 'totalHeal', 'damageSelfMitigated', 'objectDamage', 'turretDamage', 'minionCS', 
                       'monsterCS', 'teamMonsterCS', 'enemyMonsterCS', 'visionScore', 'visionWardsBought', 'wardsPlaced', 'wardsKilled',
                       'turretKills', 'inhibitorKills']
    if 'firstBloodKill' in stat:
        for_detail_list.append('firstBloodKill')
    if 'firstTowerKill' in stat:
        for_detail_list.append('firstTowerKill')



    summary_df[for_summary_list] = stat[for_summary_list]
    rune_df[for_rune_list] = stat[for_rune_list]
    detail_df[for_detail_list] = stat[for_detail_list]

    
    
    item = summary_df[['item0','item1','item2','item3','item4','item5','item6']]
    spell = summary_df[['spell1','spell2']]

    line_check_1 = []
    line_check_2 = []

    
    for i in range(min(len(frame),11)):
        minute_data = pd.DataFrame(frame.iloc[i].values(), index=frame.iloc[i].keys())
        line_check_1.append(minute_data[:5])
        line_check_2.append(minute_data[5:])
        
    

    summary_df.loc[:4,'role'] = role(item.iloc[:5], spell.iloc[:5], line_check_1)
    summary_df.loc[5:,'role'] = role(item.iloc[5:], spell.iloc[5:], line_check_2)
    
    
    
    return [summary_df, rune_df, detail_df]
