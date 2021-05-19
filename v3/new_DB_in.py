import sqlalchemy
import requests
import pandas as pd
import datetime
import asyncio
import aiohttp
from sqlalchemy import create_engine

from get_match_info import match_info
from get_personal import personal
from get_team import team
from get_event import events
from get_time import timeline


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
    
    #iter_size = 28
    iter_size = len(api_name)
    
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'    
    

    for i in range(5, 6):
        db = 'v10_%02d' % i
        engine_dict[i] = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, host, port, db) +'?charset=utf8',
        
        already_in_tuple = engine_dict.execute('SELECT gameId FROM gameSummary').fetchall()
        already_in = [gameId_tuple[0] for gameId_tuple in already_in_tuple]
        

        team_toDB = pd.DataFrame()
        time_toDB = pd.DataFrame(columns=['gameid'])
        building_toDB = pd.DataFrame(columns=['gameid'])
        
        for j in range(len(already_in)//iter_size+1):
            data_url_list, time_url_list, gameid_list = [], [], []
            for k in range(iter_size):
                if (j*iter_size+k) >= len(already_in):
                    break
                data_url_list.append(data_url+str(already_in[j*iter_size+k])+'?api_key='+api_list[k])
                time_url_list.append(time_url+str(already_in[j*iter_size+k])+'?api_key='+api_list[k])
                gameid_list.append(already_in[j*iter_size+k])
        
            data_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in data_url_list])
            data_df_list = [pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T for data_json in data_json_list]
            
            time_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in time_url_list])
            frame_df_list = [pd.DataFrame(time_json['frames'])['participantFrames'] for time_json in time_json_list]
            event_df_list = [pd.DataFrame(time_json['frames'])['events'] for time_json in time_json_list]
            

            for k in range(len(gameid_list)):
                match, frame, event = data_df_list[k], frame_df_list[k], event_df_list[k]
                
                new_event_df_list = events(event)
                building_toDB = building_toDB.append(new_event_df_list[2])
                
                new_team_df = team(match, new_event_df_list[3])
                team_toDB = team_toDB.append(new_team_df)
                
                new_minute_df = minute(frame, new_event_df_list[0])
                new_minute_df['gameID'] = now_matchid
                minute_toDB = minute_toDB.append(new_minute_df)
                
                
            print('database v10_%02d, %3d / %d th chunk updated.' % (i,j,len(already_in)//iter_size+1))
            
            
            if j % 20 == 19 or j == len(already_in)//iter_size:
                print('database v10_%02d, %2d part upload started.' % (i,j//20))
                buildling_toDB.to_sql('event_building', con=engine, index=False, if_exists='append')
                team_toDB.to_sql('team_summary', con=engine, index=False, if_exists='append')
                minute_toDB.to_sql('minute_summary', con=engine, index=False, if_exists='append')
                print('database v10_%02d, %2d part upload finished!' % (i,j//20))
                team_toDB = pd.DataFrame()
        

                
    
    
if __name__ == "__main__":
    asyncio.run(main())
