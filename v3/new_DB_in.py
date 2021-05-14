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


api_list = ['RGAPI-ae4726f1-ab03-4981-b048-e5231b028a37',    # apikokoma
            'RGAPI-1d4b3c52-fc28-463d-a281-64818b079694',    # api2kokoma
            'RGAPI-74ec36ae-6907-43a5-a241-818c4af438ef',    # api3kokoma
            'RGAPI-c885609b-5f73-4481-891c-dc1e562ab820',    # api4kokokma
            'RGAPI-674de813-3b0e-407d-9a55-d26051296d8b',    # youngcheol94
            'RGAPI-a7caa0c6-e46d-4de4-88ed-13c7fe493772',    # kjwk9900
            'RGAPI-56573211-f091-4ffb-9269-f0777672a60a',    # dhyung2002
            'RGAPI-8d3e144d-50ab-49f9-9902-fa8d2871c895',    # skdlsco2
            'RGAPI-135cb9d5-852c-4427-816e-b634c1b4410c',    # noraworld
            'RGAPI-b3d258cc-f28a-4bd7-a50f-0d790a9b6b16',    # tyaaan93
            'RGAPI-8f02b5c8-225c-4c86-8986-04b20d2bbbbe',    # marnitto89
            'RGAPI-c77d782a-2f3e-4620-9683-d222fa9ff8f9',    # dh3354
            'RGAPI-67076557-2728-4f53-b8eb-726d9fde3c34',    # dh33543354
            'RGAPI-864176f3-ba0c-4dcf-81bc-c70907328b02',    # resberg13
            'RGAPI-fe4ba430-9a3b-42f4-9b0e-5eb222196cf9',    # jyy3151
            'RGAPI-6d43ce49-02b2-45f2-bdf8-2aa841dd5735',    # archve9307
            'RGAPI-ac484806-05aa-4468-a51a-adf5831adcda',    # tipho123
            'RGAPI-088518f2-4b0c-4121-9ef2-06e979a1ff41',    # tipho1234
            'RGAPI-12de08df-3e6d-4340-b26f-96380ca14fc6',    # tipho112
            'RGAPI-6af7b842-7fb5-4071-bd82-a2b4fff781f9',    # tipho26
            'RGAPI-d8f8e1c8-2dd4-456d-811d-6e843ec5f163',    # jskim9310
            'RGAPI-10fa395c-e9e5-4d73-a82e-6e3a0f2e8fb8',    # rlatkddlf8
            'RGAPI-bfd827f8-d76f-484e-83fd-9ab3b54621e8',    # ldh123a
            'RGAPI-757ace79-f5ce-4d03-8fd8-89bef87d7ee6',    # meelmyeon
            'RGAPI-7a096b30-4dfb-4d45-b33c-e3b91c869cb9',    # dbseorms2446
            'RGAPI-244c7b8c-2e53-4da3-961f-426442aa71b7',    # dayever22
            'RGAPI-093ad6e9-8f63-4153-9526-a13facb64aab',    # bluozlz
            'RGAPI-21dc297f-eed3-4f0c-a65e-e6eed2ae15fa',    # kokoma1622
            'RGAPI-9325ea3e-fcc6-48b7-b644-73f366ea2e50',    # NHcodna2
            'RGAPI-2e8038be-db3d-4d22-912b-4f7b81e7b8c8',    # NHcodna4
            'RGAPI-6efe9460-01a1-4088-a23e-3c883596208e',    # nyjwnh
            'RGAPI-5e604ccd-6aba-4194-9169-8272eedf8745',    # dbwlssoghks
            'RGAPI-74ccc457-83fc-442c-b533-c2cdd33f8888',    # onhnyj
            'RGAPI-bf8eb878-3168-4768-9c4d-9db7fc43db6e']    # GodDrinkTeJAVA

api_name = ['apikokoma', 'api2kokoma', 'api3kokoma', 'api4kokokma', 'youngcheol94', 'kjwk9900', 'dhyung2002', 'skdlsco2',
            'noraworld', 'tyaaan93', 'marnitto89', 'dh3354', 'dh33543354', 'resberg13', 'jyy3151', 'archve9307',
            'tipho123', 'tipho1234', 'tipho112', 'tipho26', 'jskim9310', 'rlatkddlf8', 'ldh123a', 'meelmyeon', 
            'dbseorms2446', 'dayever22', 'bluozlz', 'kokoma1622', 'NHcodna2', 'NHcodna4', 'nyjwnh', 'dbwlssoghks',
            'onhnyj', 'GodDrinkTeJAVA']

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
        engine = create_engine('mysql+pymysql://kokoma:'+'qkr741963'
                               +'@challenger-match-event.cq82nctrk585.ap-northeast-2.rds.amazonaws.com:3306/v10_%02d' % i,
                               echo=False)
        
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
