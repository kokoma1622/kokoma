import pymysql
from openpyxl import Workbook
from openpyxl import load_workbook
from sqlalchemy import create_engine
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import time


api_list = ['RGAPI-86354e18-e749-4a67-8031-305c6fd460bd',    # apikokoma
            'RGAPI-c5a0e61c-e8bb-4e6b-b348-a94bbdb7b17d',    # api2kokoma
            'RGAPI-66c7af19-eca1-44ea-a889-7748f071c13a',    # api3kokoma
            'RGAPI-f0f55dff-24a2-488c-8b30-fb9ef4f9c6fc',    # api4kokoma
            'RGAPI-6fff41e5-0230-4b84-9257-a95cb0a40654',    # youngcheol94
            'RGAPI-994a299d-5308-44ad-a516-65425ffebeab',    # kjwk9900
            'RGAPI-f9a708b1-d21f-464e-acf7-c62fc437f3d9',    # dhyung2002
            'RGAPI-846d5e61-4d03-4a54-a19a-d4d99a098484',    # skdlsco2
            'RGAPI-2e40b783-c31e-4d18-8938-b6ec3fb86570',    # noraworld
            'RGAPI-b4ff5f34-0ede-4999-9687-dcf32c63745f',    # tyaaan93
            'RGAPI-1eecb141-78ad-4341-85b0-d45a6bcc2e97',    # marnitto89
            'RGAPI-025dabaf-bd92-4af8-820d-54186323bb43',    # dh3354
            'RGAPI-e7e846a5-f708-42da-96b7-09d63dfde085',    # dh33543354
            'RGAPI-bad04a6f-2e7c-4a70-b13a-73f11c7109fc',    # resberg13
            'RGAPI-ab79e713-4bf9-4194-8b3c-63da7bd7c93f',    # jyy3151
            'RGAPI-9889d6a3-5047-475e-94c3-5938f32d70e1']    # archve9307


api_dict = {'apikokoma':    'RGAPI-86354e18-e749-4a67-8031-305c6fd460bd',
            'api2kokoma':   'RGAPI-c5a0e61c-e8bb-4e6b-b348-a94bbdb7b17d',
            'api3kokoma':   'RGAPI-66c7af19-eca1-44ea-a889-7748f071c13a',
            'api4kokoma':   'RGAPI-f0f55dff-24a2-488c-8b30-fb9ef4f9c6fc',
            'youngcheol94': 'RGAPI-6fff41e5-0230-4b84-9257-a95cb0a40654',
            'kjwk9900':     'RGAPI-994a299d-5308-44ad-a516-65425ffebeab',
            'dhyung2002':   'RGAPI-f9a708b1-d21f-464e-acf7-c62fc437f3d9',
            'skdlsco2':     'RGAPI-846d5e61-4d03-4a54-a19a-d4d99a098484',
            'noraworld':    'RGAPI-2e40b783-c31e-4d18-8938-b6ec3fb86570',
            'tyaaan93':     'RGAPI-b4ff5f34-0ede-4999-9687-dcf32c63745f',
            'marnitto89':   'RGAPI-1eecb141-78ad-4341-85b0-d45a6bcc2e97',
            'dh3354':       'RGAPI-025dabaf-bd92-4af8-820d-54186323bb43',
            'dh33543354':   'RGAPI-e7e846a5-f708-42da-96b7-09d63dfde085',
            'resberg13':    'RGAPI-bad04a6f-2e7c-4a70-b13a-73f11c7109fc',
            'jyy3151':      'RGAPI-ab79e713-4bf9-4194-8b3c-63da7bd7c93f',
            'archve9307':   'RGAPI-9889d6a3-5047-475e-94c3-5938f32d70e1'}



def request_url(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            return r.json()
        
        time.sleep(5)
        print('5 seconds sleep!')



def main():

    api_key = 'RGAPI-8c43a3cd-c38f-4aa5-8846-4cda990f653b'

    drop_list = ['largestKillingSpree', 'largestMultiKill', 'killingSprees', 'longestTimeSpentLiving', 'doubleKills', 'tripleKills', 
                 'quadraKills', 'pentaKills', 'unrealKills', 'totalDamageDealt', 'magicDamageDealt', 'physicalDamageDealt', 
                 'trueDamageDealt', 'largestCriticalStrike', 'totalUnitsHealed', 'timeCCingOthers', 'totalTimeCrowdControlDealt', 
                 'sightWardsBoughtInGame', 'combatPlayerScore', 'objectivePlayerScore', 'totalPlayerScore', 'totalScoreRank', 
                 'playerScore0', 'playerScore1', 'playerScore2', 'playerScore3', 'playerScore4', 'playerScore5', 'playerScore6', 
                 'playerScore7', 'playerScore8', 'playerScore9', 'perk0Var1', 'perk0Var2', 'perk0Var3', 'perk1Var1', 'perk1Var2', 'perk1Var3', 
                 'perk2Var1', 'perk2Var2', 'perk2Var3', 'perk3Var1', 'perk3Var2', 'perk3Var3', 'perk4Var1', 'perk4Var2', 'perk4Var3', 
                 'perk5Var1', 'perk5Var2', 'perk5Var3']

    'firstBloodAssist' 'firstTowerAssist' 'firstInhibitorKill', 'firstInhibitorAssist'
    

    rename_columns = {'totalDamageDealtToChampions':'sDamageDealt', 'magicDamageDealtToChampions':'mDamageDealt',
                  'physicalDamageDealtToChampions':'pDamageDealt', 'trueDamageDealtToChampions':'tDamageDealt',
                  'damageDealtToObjectives':'objectDamage', 'damageDealtToTurrets':'turretDamage', 'totalDamageTaken':'sDamageTaken',
                  'magicalDamageTaken':'mDamageTaken', 'physicalDamageTaken':'pDamageTaken', 'trueDamageTaken':'tDamageTaken',
                  'totalMinionsKilled':'minionCS', 'neutralMinionsKilled':'monsterCS', 
                  'neutralMinionsKilledTeamJungle':'teamMonsterCS', 'neutralMinionsKilledEnemyJungle': 'enemyMonsterCS',
                  'visionWardsBoughtInGame':'visionWardsBought'}

    
    l=0
    '''
    matchid_df = pd.read_csv('c_matchid.csv')
    

    '''
    
    api_name = ['apikokoma', 'api2kokoma', 'api3kokoma', 'api4kokoma', 'youngcheol94', 'kjwk9900', 'dhyung2002', 'skdlsco2',
                'noraworld', 'tyaaan93', 'marnitto89', 'dh3354', 'dh33543354', 'resberg13', 'jyy3151', 'archve9307']
    summoner_dict = {}
    
    for api in api_name:
        summoner_dict[api] = pd.read_csv(api+'.csv')
    
    summonername_df = summoner_dict[api]['summonerName']
    
    
    
    engine = create_engine('mysql+pymysql://kokoma:'+'qkr741963'
                           +'@challenger-match-event.cq82nctrk585.ap-northeast-2.rds.amazonaws.com:3306/match_data',
                           echo=False)
    conn = engine.connect()
    
    match_already_in_tuple = engine.execute('SELECT gameId FROM match_id').fetchall()
    match_already_in = [gameId_tuple[0] for gameId_tuple in match_already_in_tuple]
    personal_already_in_tuple = engine.execute('SELECT gameId FROM personal_summary GROUP BY gameId').fetchall()
    personal_already_in = [gameId_tuple[0] for gameId_tuple in result_already_in_tuple]
    team_already_in_tuple = engine.execute('SELECT gameId FROM team_summary GROUP BY gameId').fetchall()
    team_already_in = [gameId_tuple[0] for gameId_tuple in team_already_in_tuple]
    event_already_in_tuple = engine.execute('SELECT gameId FROM event_kill GROUP BY gameId').fetchall()
    event_already_in = [gameId_tuple[0] for gameId_tuple in event_already_in_tuple]
    
    conn.close()
    engine.dispose()
    
    
    begin_index = 0
    matchid_df = pd.DataFrame()

    while True:
        match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + summonerName
        match_url += '?queue=420&beginIndex=' + str(begin_index) + '&api_key='
        match_json = request_url(match_url)

        matchid_list = pd.DataFrame(match_json['matches'])
        if matchid_list.empty == True:
            break

        matchid_df = pd.concat([matchid_df, matchid_list], ignore_index=True)

        if matchid_list['gameId'].iloc[-1] < 4071405708:
            matchid_df = matchid_df[matchid_df['gameId'] > 4071405708]
            break

        begin_index += 100
    
    
    for j in range(len(matchid_df)):
        now_matchid = matchid_df.iloc[j]['gameId'].item()
        
        data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/' + now_matchid + '?api_key='
        data_json = request_url(data_url)
        match = pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T
        
        
        if now_matchid not in match_already_in:
            summoner_ids = pd.DataFrame(dict(pd.DataFrame(match['participantIdentities'].iloc[0])['player'])).T['summonerId']
            season = match['gameVersion'].item().split('.')[0]
            version = match['gameVersion'].item().split('.')[1]

            point_sum = 0
            ranked_count = 0
            for k in range(10):
                summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/' + summoner_ids.iloc[k] + '?api_key='
                summoner_json = request_url(summoner_url)
                summoner_df = pd.DataFrame(summoner_json)

                if len(summoner_df) == 0: # unranked
                    continue

                if 'RANKED_SOLO_5x5' not in list(summoner_df['queueType']): # solo unranked
                    continue

                summoner_rank_df = summoner_df[summoner_df['queueType']=='RANKED_SOLO_5x5']
                tier = summoner_rank_df['tier'].item()
                rank = summoner_rank_df['rank'].item()

                point = tier_to_point[tier] + rank_to_point[rank]
                ranked_count += 1
                if tier == 'DIAMOND' and rank == 'I':
                    point += 1
                point_sum += point

            if ranked_count == 0:
                tier = 'UNRANKED'
            else:
                point_average = point_sum/ranked_count
                if point_average//4 < 6:
                    point_under_m = round(point_average) // 4
                    if point_under_m == 6:
                        tier = 'DIAMOND I'
                    else:
                        rank_under_m = round(point_average) % 4
                        tier = point_to_tier_under_m[point_under_m] + point_to_rank[rank_under_m]
                else:
                    point_over_m = round((point_average-24)/2)
                    tier = point_to_tier_over_m[point_over_m]

            match_dict = {'gameId':now_matchid, 'season':season, 'version':version, 'averageTier':tier}
            match_df = pd.DataFrame(match_dict, index=[0])
            match_df.to_sql('match_id', con=engine, index=False, if_exists='append')

            print(i, 'th user ' + now_matchid + ' matchid in')

    
    
    
    
    
    for k in range(len(matchid_df)-1, 0, -1):
        now_matchid = str(matchid_df.iloc[k]['gameId'].item())

        if now_matchid not in result_already_in:
            data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/' + now_matchid + '?api_key=' + api_list[l]
            data_json = request_url(data_url)
            match = pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T
            
            lane = pd.DataFrame(dict(pd.DataFrame(match['participants'].iloc[0])['timeline'])).T['lane']
            role = pd.DataFrame(dict(pd.DataFrame(match['participants'].iloc[0])['timeline'])).T['role']
            
            stat = pd.DataFrame(dict(pd.DataFrame(match['participants'].iloc[0])['stats'])).T
            if 'firstInhibitorKill' in stat:
                stat = stat.drop(drop_inhi, axis=1)
            elif 'firstBloodKill' not in stat:
                stat = stat.drop(drop_noblood, axis=1)
            elif 'firstTowerKill' not in stat:
                stat = stat.drop(drop_notower, axis=1)
            else:
                stat = stat.drop(drop_list, axis=1)
            stat.rename(columns=rename_columns, inplace=True)
            stat.insert(0, 'gameId', now_matchid)
            stat['lane'] = lane
            stat['role'] = role

            stat.to_sql('match_data_result', con=engine, index=False, if_exists='append')
            print(now_matchid, 'result inserted!')
            
        else:
            print(now_matchid, 'result already in!')


            
        if now_matchid not in timedata_already_in:
            time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/' + now_matchid + '?api_key=' + api_list[l]
            time_json = request_url(time_url)
            event = pd.DataFrame(time_json['frames'])['events']
            
            
            time_data = [[] for _ in range(10)]

            for i in range(len(frame)):
                for key in key_list:
                    participant_id = frame[i]['participantFrames'][key]['participantId']
                    user_data = {'gameId': now_matchid,
                                 'participantId': frame[i]['participantFrames'][key]['participantId'],
                                 'minute' : i,
                                 'currentGold': frame[i]['participantFrames'][key]['currentGold'],
                                 'totalGold': frame[i]['participantFrames'][key]['totalGold'], 
                                 'level': frame[i]['participantFrames'][key]['level'],
                                 'xp': frame[i]['participantFrames'][key]['xp'],
                                 'minionCS': frame[i]['participantFrames'][key]['minionsKilled'],
                                 'monsterCS': frame[i]['participantFrames'][key]['jungleMinionsKilled']}

                    time_data[participant_id-1].append(user_data)
            
            match_time_df = pd.DataFrame(time_data).T
            match_time_df.to_sql('match_data_time', con=engine, index=False, if_exists='append')
            print(now_matchid, 'm_data inserted!')
            
        else:
            print(now_matchid, 'result already in!')


            
        if now_matchid not in event_already_in:
            time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/' + now_matchid + '?api_key=' + api_list[l]
            time_json = request_url(time_url)
            event = pd.DataFrame(time_json['frames'])['events']


            for i in range(len(event)):
                for j in range(len(event.iloc[i])):
                    if 'position' in event.iloc[i][j]:
                        event.iloc[i][j]['positionX'] = event.iloc[i][j]['position']['x']
                        event.iloc[i][j]['positionY'] = event.iloc[i][j]['position']['y']
                        del event.iloc[i][j]['position']

                    if 'assistingParticipantIds' in event.iloc[i][j]:
                        event.iloc[i][j]['assistIds'] = ' '.join(map(str, event.iloc[i][j]['assistingParticipantIds']))
                        del event.iloc[i][j]['assistingParticipantIds']

            match_event_list = []
            for i in range(len(event)):
                match_event_list += event.iloc[i]

            match_event_df = pd.DataFrame(match_event_list)
            match_event_df = match_event_df[match_event_df.wardType != 'UNDEFINED']
            match_event_df.insert(0, 'gameId', now_matchid)
            match_event_df.rename(columns = {'afterId': 'from_itemId', 'beforeId': 'to_itemId'}, inplace=True)

            match_event_df.to_sql('match_event', con=engine, index=False, if_exists='append')
            print(now_matchid, 'events inserted!')
            
        else:
            print(now_matchid, 'events already in!')


        l += 1
        if l == 13:
            l = 7

            
    conn.close()
    engine.dispose()
    
    
if __name__ == "__main__":
    main()
