import pymysql
import sqlalchemy
import requests
import pandas as pd
import time
import datetime
import asyncio
import aiohttp
from sqlalchemy import create_engine

from get_match_info import match_info
from get_personal import personal
from get_team import team
from get_event import events
from get_minute import minute


api_list = []
api_name = []
api_dict = dict(zip(api_name, api_list))



async def quest_df_async(url):
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, timeout=timeout)
            while True:
                if response.status == 200:
                    text = await response.json()
                    return pd.DataFrame(text)
                elif response.status == 504:
                    await asyncio.sleep(0.2)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(30.0)
                    #print('request limit.', url)
                    response = await session.get(url)
        except:
            text = request_url(url)
            return pd.DataFrame(text)
        
async def quest_json_async(url):
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, timeout=timeout)
            while True:
                if response.status == 200:
                    text = await response.json()
                    return text
                elif response.status == 504:
                    await asyncio.sleep(0.2)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(40.0)
                    #print('request limit.', url)
                    response = await session.get(url)
        except:
            text = request_url(url)
            return text


def request_url(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(url)
            return r.json()
        elif r.status_code == 504:
            time.sleep(0.2)
        else:
            time.sleep(20)
            #print(url)
            #print('20 seconds sleep!')



async def main():
    summoner_name = pd.read_csv('../summoner_api/')['summonerName']
    summoner_series = pd.read_csv('../summoner_api/')['accountId']
    
    
    engine_dict, conn_dict = {}, {}
    already_in = []
    
    for i in range(1, 26):
        db = 'v10_%02d' % i
        engine_dict[i] = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, host, port, db) +'?charset=utf8',
                               echo=False)
        conn_dict[i] = engine_dict[i].connect()
        
        already_in_tuple = engine_dict[i].execute('SELECT gameID FROM gameSummary').fetchall()
        already_in += [gameId_tuple[0] for gameId_tuple in already_in_tuple]
        
        
    iter_size = len(api_name)
    
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'
    summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/'
    
    
    for i in range(1181, len(summoner_series)):
        matchid_df = pd.DataFrame()
        begin_index = 0
        
        while True:
            match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + summoner_series[i]
            match_url += '?queue=420&beginIndex=' + str(begin_index) + '&api_key=' + api_list[0]
            match_json = request_url(match_url)

            matchid_list = pd.DataFrame(match_json['matches'])
            will_break = 0
            if len(matchid_list) == 0:
                break
                
            matchid_df = pd.concat([matchid_df, matchid_list], ignore_index=True)
            
            if matchid_list.iloc[-1]['gameId'] < 4071405708:
                break
            
            begin_index += 100
       
        matchid_df = matchid_df[matchid_df['gameId'] > 4071405708]
        gameid_df = list(matchid_df['gameId'])
        total_game = len(gameid_df)

        gameid_df = [gameid for gameid in gameid_df if gameid not in already_in]
        uninserted_game = len(gameid_df)
        announce = 'summoner %s total %d game played, %d games not yet inserted, %d chunks needed to be inserted.' \
                    % (summoner_name[i], total_game, uninserted_game, (uninserted_game-1)//iter_size+1)
        print(announce)
        

        game_toDB, event_toDB, personal_toDB, team_toDB, time_toDB = {}, {}, {}, {}, {}
        for j in range(1, 26):
            game_toDB[j] = pd.DataFrame()
            event_toDB[j] = [pd.DataFrame()] * 4
            personal_toDB[j] = [pd.DataFrame()] * 3
            team_toDB[j] = pd.DataFrame()
            time_toDB[j] = pd.DataFrame()
        
        
        for j in range(uninserted_game//iter_size+1):
            data_url_list, time_url_list, gameid_list = [], [], []
            for k in range(iter_size):
                if (j*iter_size+k) >= uninserted_game:
                    break
                data_url_list.append(data_url+str(gameid_df[j*iter_size+k])+'?api_key='+api_list[k])
                time_url_list.append(time_url+str(gameid_df[j*iter_size+k])+'?api_key='+api_list[k])
                gameid_list.append(gameid_df[j*iter_size+k])
        
            data_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in data_url_list])
            data_df_list = [pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T for data_json in data_json_list]
            
            time_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in time_url_list])
            time_df_list = [pd.DataFrame(time_json['frames']) for time_json in time_json_list]
            
            frame_df_list = [pd.DataFrame(time_json['frames'])['participantFrames'] for time_json in time_json_list]
            event_df_list = [pd.DataFrame(time_json['frames'])['events'] for time_json in time_json_list]
            
            summoner_url_list = []
            for k in range(len(gameid_list)):
                match = data_df_list[k]
                summoner_ids = pd.DataFrame(match['participantIdentities'][0]['player']).T['summonerId']
                
                summoner_ids = pd.DataFrame(dict(pd.DataFrame(match['participantIdentities'][0])['player'])).T['summonerId']
                
                summoner_url_list += [summoner_url+summonerId+'?api_key='+api_list[k] for summonerId in summoner_ids]
            summoner_list = await asyncio.gather(*[quest_df_async(urls) for urls in summoner_url_list])
                
            for k in range(len(gameid_list)):
                now_matchid = gameid_list[k]
                match, frame, event = data_df_list[k], frame_df_list[k], event_df_list[k]
                
                
                version = int(match['gameVersion'].item().split('.')[1])
                
                new_match_df = match_info(match, summoner_list)
                game_toDB[version] = game_toDB[version].append(new_match_df)

                new_event_df_list = events(event)
                for ii in range(4):
                    new_event_df_list[ii]['gameID'] = now_matchid
                    event_toDB[version][ii] = event_toDB[version][ii].append(new_event_df_list[ii])

                new_personal_df_list = personal(match, frame, new_event_df_list[4])       
                for ii in range(3):
                    personal_toDB[version][ii] = personal_toDB[version][ii].append(new_personal_df_list[ii])

                new_team_df = team(match, new_event_df_list[3])
                team_toDB[version] = team_toDB[version].append(new_team_df)
                    
                new_time_df = minute(frame, new_event_df_list[0])
                new_time_df['gameID'] = now_matchid
                time_toDB[version] = time_toDB[version].append(new_time_df)

            print(i, 'th summoner %2d th chunk updated.' % j)
            
            
            
        print(i, 'th summoner database upload started.')
        for j in range(1, 26):
            game_toDB[j].to_sql('gameSummary', con=engine_dict[j], index=False, if_exists='append')
            event_toDB[j][0].to_sql('eventKill', con=engine_dict[j], index=False, if_exists='append')
            event_toDB[j][1].to_sql('eventItem', con=engine_dict[j], index=False, if_exists='append')
            event_toDB[j][2].to_sql('eventBuilding', con=engine_dict[j], index=False, if_exists='append')
            event_toDB[j][3].to_sql('eventMonster', con=engine_dict[j], index=False, if_exists='append')
            personal_toDB[j][0].to_sql('personalSummary', con=engine_dict[j], index=False, if_exists='append')
            personal_toDB[j][1].to_sql('personalRune', con=engine_dict[j], index=False, if_exists='append')
            personal_toDB[j][2].to_sql('personalDetail', con=engine_dict[j], index=False, if_exists='append')
            team_toDB[j].to_sql('teamSummary', con=engine_dict[j], index=False, if_exists='append')
            time_toDB[j].to_sql('minuteSummary', con=engine_dict[j], index=False, if_exists='append')
        print(i, 'th summoner database upload finished!\n')

        
        already_in += gameid_df
        
        

    
if __name__ == "__main__":
    asyncio.run(main())
