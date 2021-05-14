import pymysql
from openpyxl import Workbook
from openpyxl import load_workbook
from sqlalchemy import create_engine
import sqlalchemy

import requests
import pandas as pd
import numpy as np
import time


api_list = ['RGAPI-c9126bf7-248f-4a5d-af46-8ea737ac062f',
            'RGAPI-b4259357-78f9-442e-92ba-b83b04e292bb',
            'RGAPI-ccd8711d-00d9-45f3-ad51-ffb5c3cbb616',
            'RGAPI-a2e66bbc-5725-434c-b12f-1ec7058f667c',
            'RGAPI-e936e47f-e772-4b2f-a115-2d9e9b130671',
            'RGAPI-5fc57085-ee27-4ee8-98b2-98a68e3bfad4',
            'RGAPI-28b52999-48fe-4c0b-9447-fa823d38bb0a',
            'RGAPI-58a29c4c-cb69-4dce-bfdc-0d483d16ddf9',
            'RGAPI-310502c0-110a-493a-bd32-0e03b0bfd7f7',
            'RGAPI-931b3622-d0a7-48e1-9533-0bf3df5201d6',
            'RGAPI-053a1309-e128-4f14-954c-193259bad771',
            'RGAPI-8b5386e7-70e8-4ff2-b925-63401c7ea72d',
            'RGAPI-3093865b-393e-464f-a36e-4f8c08099552',
            'RGAPI-077b9a63-170b-4d69-b7a1-2a0a0fea8462',
            'RGAPI-9b463e00-bb76-4248-ba3a-473e50bbad9d',
            'RGAPI-ab0190c8-ba87-48d1-8fa1-7ccd6eab2f54']



def request_url(url_front):
    while True:
        for i in range(len(api_list)):
            url = url_front + api_list[i]
            r = requests.get(url)

            if r.status_code == 200:
                return r.json()
            elif r.status_code == 403: # api 갱신 필요
                print('you need api renewal')
                print(api_list[i])
                return r.json()



def main():
    engine = create_engine('mysql+pymysql://kokoma:'+'qkr741963'
                           +'@challenger-match-event.cq82nctrk585.ap-northeast-2.rds.amazonaws.com:3306/matches',
                           echo=False)
    conn = engine.connect()

    api_key = 'RGAPI-fc2c0ad1-27cf-4ce4-8867-467593774ada'


    tier_to_point = {'IRON':0, 'BRONZE':4, 'SILVER':8, 'GOLD':12, 'PLATINUM':16, 'DIAMOND':20, 'MASTER':23, 'GRANDMASTER':25, 'CHALLENGER':27}
    rank_to_point = {'IV':0, 'III':1, 'II':2, 'I':3}
    point_to_tier_under_m = {0:'IRON', 1:'BRONZE', 2:'SILVER', 3:'GOLD', 4:'PLATINUM', 5:'DIAMOND'}
    point_to_rank = {0:' IV', 1:' III', 2:' II', 3:' I'}
    point_to_tier_over_m = {0:'DIAMOND I', 1:'MASTER', 2:'GRANDMASTER', 3:'CHALLENGER'}
    

    """
    c = 'https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key=' + api_key
    c_r = requests.get(c)
    c_user_df = pd.DataFrame(c_r.json())
    c_entries_df = pd.DataFrame(dict(c_user_df['entries'])).T 
    c_summoner_df = c_entries_df['summonerId']
    c_account_df = pd.DataFrame()
    
    for i in range(len(c_summoner_df)):
        summoner_url = 'https://kr.api.riotgames.com/lol/summoner/v4/summoners/' + c_summoner_df.iloc[i] + '?api_key='
        summoner_json = request_url(summoner_url)
        c_account_df = pd.concat([c_account_df, pd.DataFrame(summoner_json, index=[0])])

    c_account_df = c_account_df['accountId']
    c_account_df.to_csv('c_accountid.csv')
    print('csv file saved!')
    """
    
    # c_matchid_df : 챌린저 accountid를 이용하여 챌린저 랭크 게임의 matchid를 뽑아내 중복 제거
    # ?beginIndex=100 이걸 통해 더 전의 데이터도 확인 가능

    # 4071405708 gameid는 9.24
    # 4071420188 gameid는 10.1


    for i in range(len(c_account_df)):
        begin_index = 0
        matchid_df = pd.DataFrame()
        
        while True:
            match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + c_account_df.iloc[i]
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
            now_matchid = str(matchid_df.iloc[j]['gameId'].item())
            check_match_exist = engine.execute('SELECT gameId FROM match_id WHERE gameId='+now_matchid).fetchall()
            if check_match_exist != []:
                print(now_matchid + ' already in')
                continue


            data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/' + now_matchid + '?api_key='
            data_json = request_url(data_url)

            match = pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T
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
    
if __name__ == "__main__":
    main()
