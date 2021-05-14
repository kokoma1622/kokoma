import pymysql
from openpyxl import Workbook
from openpyxl import load_workbook
from sqlalchemy import create_engine
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import time


api_list = ['RGAPI-c1efc91d-54e9-47b7-9034-ca245fde730a',
            'RGAPI-52fb77af-2b17-4396-9a16-74deafe3bd69',
            'RGAPI-bbf0510b-8583-4d62-8b76-7e4342044f1c',
            'RGAPI-37558a4e-f0f2-4e6e-a851-ff0f7584ec90',
            'RGAPI-05434be6-979e-453f-9b7b-7f089b16d226',
            'RGAPI-36280109-2559-4256-a634-11478300544c',
            'RGAPI-f0cdee50-5c8e-43bf-b2d1-b04efa46515e',
            'RGAPI-0605ec83-ebf5-4cbf-ac6e-327af5bcd678',
            'RGAPI-e0a608b9-c004-4ea2-b706-47dba0129e38',
            'RGAPI-c7ec745c-fe72-42fc-bc68-8debf5d52809',
            'RGAPI-d8af4e55-f0b4-453f-a538-fefc544e5d7f',
            'RGAPI-805aa6fc-5b31-4836-9582-47821742c1d5',
            'RGAPI-4c10a087-ad1a-4ba1-9f44-6611667231f5',
            'RGAPI-c3773ecb-ade0-4fea-8337-aab17fa331b1',
            'RGAPI-b814dc8d-2553-471d-bd32-dcfc51b50353']
#            'RGAPI-d0adf80f-be56-400e-9fd7-890bbf4bb7d6']


api_dict = {'apikokoma':    'RGAPI-c1efc91d-54e9-47b7-9034-ca245fde730a',
            'api2kokoma':   'RGAPI-52fb77af-2b17-4396-9a16-74deafe3bd69',
            'api3kokoma':   'RGAPI-bbf0510b-8583-4d62-8b76-7e4342044f1c',
            'api4kokoma':   'RGAPI-37558a4e-f0f2-4e6e-a851-ff0f7584ec90',
            'youngcheol94': 'RGAPI-05434be6-979e-453f-9b7b-7f089b16d226',
            'kjwk9900':     'RGAPI-36280109-2559-4256-a634-11478300544c',
            'dhyung2002':   'RGAPI-f0cdee50-5c8e-43bf-b2d1-b04efa46515e',
            'skdlsco2':     'RGAPI-0605ec83-ebf5-4cbf-ac6e-327af5bcd678',
            'noraworld':    'RGAPI-e0a608b9-c004-4ea2-b706-47dba0129e38',
            'tyaaan93':     'RGAPI-c7ec745c-fe72-42fc-bc68-8debf5d52809',
            'marnitto89':   'RGAPI-d8af4e55-f0b4-453f-a538-fefc544e5d7f',
            'dh3354':       'RGAPI-805aa6fc-5b31-4836-9582-47821742c1d5',
            'dh33543354':   'RGAPI-4c10a087-ad1a-4ba1-9f44-6611667231f5',
            'resberg13':    'RGAPI-c3773ecb-ade0-4fea-8337-aab17fa331b1',
            'jyy3151':      'RGAPI-b814dc8d-2553-471d-bd32-dcfc51b50353'}
#            'archve9307': 'RGAPI-d0adf80f-be56-400e-9fd7-890bbf4bb7d6}



def request_url(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(api)
            return r.json()



def main():
    engine = create_engine('mysql+pymysql://kokoma:'+'qkr741963'
                           +'@challenger-match-event.cq82nctrk585.ap-northeast-2.rds.amazonaws.com:3306/matches',
                           echo=False)
    conn = engine.connect()

    api_key = 'RGAPI-8c43a3cd-c38f-4aa5-8846-4cda990f653b'

    tier_to_point = {'IRON':0, 'BRONZE':4, 'SILVER':8, 'GOLD':12, 'PLATINUM':16, 'DIAMOND':20, 'MASTER':23, 'GRANDMASTER':25, 'CHALLENGER':27}
    rank_to_point = {'IV':0, 'III':1, 'II':2, 'I':3}
    
    point_to_tier_under_m = {0:'IRON', 1:'BRONZE', 2:'SILVER', 3:'GOLD', 4:'PLATINUM', 5:'DIAMOND'}
    point_to_rank = {0:' IV', 1:' III', 2:' II', 3:' I'}
    point_to_tier_over_m = {0:'DIAMOND I', 1:'MASTER', 2:'GRANDMASTER', 3:'CHALLENGER'}
    
    
    matchid_df = pd.read_csv('c_matchid.csv')
    
    gameid_already_in_tuple = engine.execute('SELECT gameId FROM match_id GROUP BY gameId').fetchall()
    gameid_already_in = [gameId_tuple[0] for gameId_tuple in gameid_already_in_tuple]
    
    
    i = 0
    for j in range(53000, 68600):
        now_matchid = str(matchid_df.iloc[j]['gameId'].item())
        
        if now_matchid in gameid_already_in:
            print(now_matchid + ' already in')
            continue


        data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/' + now_matchid + '?api_key=' + str(api_list[i])
        data_json = request_url(data_url)

        match = pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T
        summoner_ids = pd.DataFrame(dict(pd.DataFrame(match['participantIdentities'].iloc[0])['player'])).T['summonerId']
        season = match['gameVersion'].item().split('.')[0]
        version = match['gameVersion'].item().split('.')[1]


        point_sum = 0
        ranked_count = 0
        for k in range(10):
            summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/' + summoner_ids.iloc[k] + '?api_key=' + str(api_list[i])
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

        print(now_matchid + ' matchid in')
        
        i += 1
        if i == 7:
            i = 0
            
            
    conn.close()
    
if __name__ == "__main__":
    main()
